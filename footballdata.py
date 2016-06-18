import simplejson as json
import requests
import logging
from urlparse import urljoin
import time
from collections import OrderedDict
import functools
from os import getenv

from expiringdict import ExpiringDict

logger = logging.getLogger(__name__)


class Client(object):
    AUTH_TOKEN = getenv('FOOTBALL_DATA_AUTH_TOKEN')
    BASE_URL = 'http://api.football-data.org'
    API_VERSION = 'v1'

    def __init__(self, auth_token=None, base_url=None, api_version=None):
        self.AUTH_TOKEN = auth_token or self.AUTH_TOKEN
        self.BASE_URL = base_url or self.BASE_URL
        self.API_VERSION = api_version or self.API_VERSION
        self.session = requests.Session()
        self.session.headers = OrderedDict(self.session.headers)
        self.session.headers.update({'X-Auth-Token': self.AUTH_TOKEN, 'X-Response-Control': 'minified'})
        self._cache = ExpiringDict(max_len=1000, max_age_seconds=600)

    def _get_url(self, url_path, base_url=None, api_version=None):
        base_url = base_url or self.BASE_URL
        api_version = api_version or self.API_VERSION
        return urljoin('{}/{}'.format(base_url, api_version), url_path)

    @staticmethod
    def _log_request(request):
        data = {
            'method': request.method,
            'auth': request.auth,
            'cookies': request.cookies,
            'data': request.data,
            'files': request.files,
            'headers': request.headers,
            'json': request.json,
            'params': request.params,
            'url': request.url,
            }
        logger.debug('Request {method} {url}: {}'.format(data, **data))

    @staticmethod
    def _log_response(response):
        data = {
            'content': response.content,
            'cookies': response.cookies,
            'elapsed': response.elapsed,
            'encoding': response.encoding,
            'headers': response.headers,
            'history': response.history,
            'is_permanent_redirect': response.is_permanent_redirect,
            'is_redirect': response.is_redirect,
            'links': response.links,
            'method': response.request.method,
            'reason': response.reason,
            'status_code': response.status_code,
            'url': response.url,
            }
        logger.debug('Response {method} {url}: {}'.format(data, **data))

    def _perform_request(self, url_path, method=None, base_url=None, api_version=None, data=None, headers=None):
        method = method or 'GET'
        base_url = base_url or self.BASE_URL
        api_version = api_version or self.API_VERSION
        headers = headers or self.session.headers
        url = self._get_url(url_path, base_url, api_version)
        header_tuple = tuple(headers.items())
        cache_key = (method, url, data, header_tuple)
        if cache_key in self._cache:
            return self._cache[cache_key]
        req = requests.Request(method, self._get_url(url_path, base_url, api_version), data=data, headers=headers)
        self._log_request(req)
        prepped = req.prepare()
        resp = self.session.send(prepped)
        self._log_response(resp)
        self._cache[cache_key] = resp
        return resp

    def get_seasons(self):
        url_path = 'soccerseasons/'
        return self._perform_request(url_path).json()

    def get_season(self, season_id):
        url_path = 'soccerseasons/{}'.format(season_id)
        return self._perform_request(url_path).json()

    def get_teams(self, season_id):
        url_path = 'soccerseasons/{}/teams'.format(season_id)
        return self._perform_request(url_path).json()

    def get_league_table(self, season_id):
        url_path = 'soccerseasons/{}/leagueTable'.format(season_id)
        return self._perform_request(url_path).json()

    def get_group(self, season_id, group):
        table = self.get_league_table(season_id)
        return table['standings'][group]

    def get_fixtures(self, season_id, matchday=None):
        url_path = 'soccerseasons/{}/fixtures'.format(season_id)
        if matchday is not None:
            url_path += '?matchday={}'.format(matchday)
        return self._perform_request(url_path).json()

    def get_fixture(self, season_id, fixture_id):
        url_path = 'soccerseasons/{}/fixtures/{}'.format(season_id, fixture_id)
        return self._perform_request(url_path).json()

    def get_team(self, team_id):
        url_path = 'teams/{}'.format(team_id)
        return self._perform_request(url_path).json()


class SeasonClient(Client):
    def __init__(self, season_id, *args, **kwargs):
        self.season_id = season_id
        super(SeasonClient, self).__init__(*args, **kwargs)

    def get_season(self):
        return super(self.__class__, self).get_season(self.season_id)

    def get_teams(self):
        return super(self.__class__, self).get_teams(self.season_id)

    def get_league_table(self):
        return super(self.__class__, self).get_league_table(self.season_id)

    def get_group(self, group):
        return self.get_league_table()['standings'][group]

    def get_fixtures(self, matchday=None):
        return super(self.__class__, self).get_fixtures(self.season_id, matchday)

    def get_fixture(self, fixture_id):
        return super(self.__class__, self).get_fixture(self.season_id, fixture_id)
