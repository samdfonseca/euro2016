from collections import OrderedDict
import logging
import time
from datetime import datetime
from Queue import Queue
from threading import Thread
import os
from StringIO import StringIO
import re

from footballdata import SeasonClient

import simplejson as json
from sheetsync import Sheet, ia_credentials_helper
from requests.exceptions import ConnectionError
from dateutil.tz import tzutc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger('sheetsync').setLevel(logging.DEBUG)
logging.getLogger('footballdata').setLevel(logging.DEBUG)
logging.basicConfig()


team_abbreviations = OrderedDict({
        'FRA': 773,
        'SWI': 788,
        'ALB': 1065,
        'ROM': 811,
        'ENG': 770,
        'WAL': 833,
        'SVK': 768,
        'RUS': 808,
        'GER': 759,
        'POL': 794,
        'NI': 829,
        'UKR': 790,
        'SPA': 760,
        'CRO': 799,
        'CZE': 798,
        'TUR': 803,
        'ITA': 784,
        'IRE': 806,
        'SWE': 792,
        'BEL': 805,
        'HUN': 827,
        'POR': 765,
        'ICE': 1066,
        'AUT': 816,
        })
team_ids = OrderedDict(dict(zip(team_abbreviations.values(), team_abbreviations.keys())))


class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.fatal(e)
            finally:
                self.tasks.task_done()


class ThreadPool(object):
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kwargs):
        self.tasks.put((func, args, kwargs))

    def map(self, func, arg_list):
        for args in arg_list:
            self.add_task(func, *args)

    def wait_completion(self):
        self.tasks.join()

class cached_property(object):
    '''Decorator for read-only properties evaluated only once within TTL period.

    It can be used to create a cached property like this::

        import random

        # the class containing the property must be a new-style class
        class MyClass(object):
            # create property whose value is cached for ten minutes
            @cached_property(ttl=600)
            def randint(self):
                # will only be evaluated every 10 min. at maximum.
                return random.randint(0, 100)

    The value is cached  in the '_cache' attribute of the object instance that
    has the property getter method wrapped by this decorator. The '_cache'
    attribute value is a dictionary which has a key for every property of the
    object which is wrapped by this decorator. Each entry in the cache is
    created only when the property is accessed for the first time and is a
    two-element tuple with the last computed property value and the last time
    it was updated in seconds since the epoch.

    The default time-to-live (TTL) is 300 seconds (5 minutes). Set the TTL to
    zero for the cached value to never expire.

    To expire a cached property value manually just do::

        del instance._cache[<property name>]

    '''
    def __init__(self, ttl=300):
        self.ttl = ttl

    def __call__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__
        return self

    def __get__(self, inst, owner):
        now = time.time()
        try:
            value, last_update = inst._cache[self.__name__]
            if self.ttl > 0 and now - last_update > self.ttl:
                raise AttributeError
        except (KeyError, AttributeError):
            value = self.fget(inst)
            try:
                cache = inst._cache
            except AttributeError:
                cache = inst._cache = {}
            cache[self.__name__] = (value, now)
        return value

class EuroCupBracketWorkbook(object):
    DOCUMENT_NAME = 'Euro Cup 2016'
    _RETRY_BACKOFF = 2

    def __init__(self, document_name=None, credential_cache_file=None):
        self.DOCUMENT_NAME = document_name or self.DOCUMENT_NAME
        self.CLIENT_ID = os.getenv('GOOGLE_API_CLIENT_ID')
        self.CLIENT_SECRET = os.getenv('GOOGLE_API_CLIENT_SECRET')
        self.CREDENTIAL_CACHE = os.getenv('GOOGLE_API_CREDENTIAL_CACHE')
        self.credential_cache_file = credential_cache_file or 'cred_cache.json'
        if self.CREDENTIAL_CACHE is not None and not os.path.exists(self.credential_cache_file):
            with open(self.credential_cache_file, 'w') as f:
                f.write(self.CREDENTIAL_CACHE)
        self.credentials = ia_credentials_helper(
                self.CLIENT_ID,
                self.CLIENT_SECRET,
                credentials_cache_file=self.credential_cache_file)
        try:
            self.wbk = Sheet(credentials=self.credentials, document_name=self.DOCUMENT_NAME)
        except ConnectionError:
            time.sleep(EuroCupBracketWorkbook._RETRY_BACKOFF)
            EuroCupBracketWorkbook._RETRY_BACKOFF **= 2
            self.__init__(document_name, credential_cache_file)
        self.sheets = self.wbk.sheet.worksheets()
        self.data_api_client = SeasonClient(season_id='424')
        self.scoring_sheet = EuroCupBracketScoringSheet(self)
        self.participant_sheets = [EuroCupBracketGroupStagePicksSheet(i, self) for i in
                self.scoring_sheet.participants]
        self._worker_pool = ThreadPool(10)

    def get_sheet(self, sheet_name):
        return filter(lambda sheet: sheet.title == sheet_name, self.sheets)[0]

    def does_team_advance(self, team_id):
        team_groups = {}
        table = self.data_api_client.get_league_table()
        for group in table['standings'].keys():
            for team in table['standings'][group]:
                team_groups[team['teamId']] = group
        group = self.data_api_client.get_group(team_groups[team_id])
        if group[0]['teamId'] == team_id:
            return True
        if group[1]['teamId'] == team_id:
            return True
        if team_id in self.get_third_place_advancers():
            return True
        return False
        
    def get_third_place_teams(self):
        thirds = [group[2] for group in self.data_api_client.get_league_table()['standings'].values()]
        return sorted(thirds, key=lambda i: i['points'], reverse=True)

    def get_third_place_advancers(self):
        def sort_teams_by(teams, attr, reverse=True):
            return sorted(candidates, lambda i: i[attr], reverse=reverse)
        def filter_teams_by(teams, **kwargs):
            for k, v in kwargs.iteritems():
                teams = filter(lambda i: i[k] == v, teams)
            return teams
        def unique_values(teams, attr):
            return set([i[attr] for i in teams])
        thirds = self.get_third_place_teams()
        thirds.sort(key=lambda i: (i['points'], i['goalDifference'], i['goals'], i['goalsAgainst']*-1))
        return [i['teamId'] for i in thirds[:4]]

    @staticmethod
    def score_participant_group_stage(participant_sheet, scoring_sheet):
        score = participant_sheet.calculate_score()
        logger.debug('Participant: {}, Score: {}'.format(participant_sheet.worksheet_name, score))
        scoring_sheet.set_participant_group_stage_score(participant_sheet.worksheet_name, score)

    def score_participants_group_stage(self):
        args = [(sht, self.scoring_sheet) for sht in self.participant_sheets]
        self._worker_pool.map(self.score_participant_group_stage, args)
        self._worker_pool.wait_completion()


class EuroCupBracketScoringSheet(object):
    WORKSHEET_NAME = 'Scoring'
    _RETRY_BACKOFF = 2

    def __init__(self, workbook, worksheet_name=None):
        self.WORKSHEET_NAME = worksheet_name or self.WORKSHEET_NAME
        self.wbk = workbook
        try:
            self.sheet = self.wbk.get_sheet(self.WORKSHEET_NAME)
        except ConnectionError:
            time.sleep(self._RETRY_BACKOFF)
            EuroCupBracketScoringSheet._RETRY_BACKOFF **= 2
            self.__init__(workbook)

    @cached_property(ttl=600)
    def participants(self):
        return filter(lambda i: i != '', self.sheet.col_values(1))

    def participant(self, participant_name):
        return filter(lambda i: i.value == participant_name,
                self.sheet.range('A2:A{}'.format(len(self.participants)+1)))[0]

    def set_participant_group_stage_score(self, participant, score):
        cell = self.participant(participant)
        self.sheet.update_cell(cell.row, cell.col+1, score)
        self.sheet.update_cell(cell.row, cell.col+4, datetime.now(tzutc()).strftime('%a %b %d %H:%M:%S %Z %Y'))

    def set_participant_bracket_score(self, participant, score):
        cell = self.participant(participant)
        self.sheet.update_cell(cell.row, cell.col+2, score)
        self.sheet.update_cell(cell.row, cell.col+4, datetime.now(tzutc()).strftime('%a %b %d %H:%M:%S %Z %Y'))


class EuroCupBracketGroupStagePicksSheet(object):
    _RETRY_BACKOFF = 2
    GROUP_RANGES = {
            'A': 'C1:C3',
            'B': 'C7:C9',
            'C': 'C13:C15',
            'D': 'C19:C21',
            'E': 'C25:C27',
            'F': 'C31:C33',
            }

    def __init__(self, worksheet_name, workbook):
        self.worksheet_name = worksheet_name
        self.wbk = workbook
        try:
            self.sheet = self.wbk.get_sheet(self.worksheet_name)
        except ConnectionError:
            time.sleep(self._RETRY_BACKOFF)
            EuroCupBracketGroupStagePicksSheet **= 2
            self.__init__(worksheet_name, workbook)

    def get_group_pics(self, group):
        cells = self.sheet.range(self.GROUP_RANGES[group])
        return map(lambda cell: cell.value, cells)

    def calculate_group_score(self, group):
        standings = self.wbk.data_api_client.get_group(group)
        picks = filter(lambda i: i != '', self.get_group_pics(group))
        standing_ids = map(lambda i: i['teamId'], standings)
        pick_ids = map(lambda i: team_abbreviations[i], picks)
        score = 0
        for i, pick_id in enumerate(pick_ids):
            if self.wbk.does_team_advance(pick_id):
                score += 1
                if standing_ids[i] == pick_id:
                    score += 1
        return score

    def calculate_score(self):
        return sum([self.calculate_group_score(i) for i in ['A','B','C','D','E','F']])


if __name__ == '__main__':
    wbk = EuroCupBracketWorkbook()
    wbk.score_participants_group_stage()
    finished_fixtures = filter(lambda i: i['status'] == 'FINISHED',
            wbk.data_api_client.get_fixtures()['fixtures'])
    while True:
        _finished_fixtures = filter(lambda i: i['status'] == 'FINISHED',
                wbk.data_api_client.get_fixtures()['fixtures'])
        if len(_finished_fixtures) != len(finished_fixtures):
            wbk.score_participants_group_stage()
            finished_fixtures = _finished_fixtures
        if len(filter(lambda i: i['status'] != 'FINISHED', wbk.data_api_client.get_fixtures()['fixtures'])) == 0:
            break
        time.sleep(20*60)
        
