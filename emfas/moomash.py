from __future__ import unicode_literals

import json
import requests
import logging


logger = logging.getLogger('emfas')


class MoomashAPIException(Exception):
    def __init__(self, message, code, version):
        Exception.__init__(self, message)

        self.message = message
        self.code = code
        self.version = version

    @classmethod
    def from_json(cls, j):
        return cls(
            j['message'], j['code'], j['version']
        )


class MoomashAPI(object):
    BASE_URL = 'http://api.mooma.sh/v1'
    HEADERS = {
        'Content-Type': 'application/octet-stream'
    }

    def __init__(self, api_key):
        self.api_key = api_key

    def identify(self, payload):
        if not isinstance(payload, (str, bytes)):
            payload = json.dumps(payload)

        url = '{0}/song/identify'.format(self.BASE_URL)
        response = requests.post(
            url, data=payload, headers=self.HEADERS,
            params=[('api_key', self.api_key)]
        )
        j = response.json()
        logger.debug('Moomash response: {0!r}'.format(j))
        if not response.ok:
            raise MoomashAPIException.from_json(j)

        # sometimes songs looks like that...
        # {u'songs': [{}]}
        return [Song.from_json(song) for song in j['response']['songs'] if song]


class Song(object):
    def __init__(self, artist_id=None, artist_name=None, id=None, score=0, title=None):
        self.artist_id = artist_id
        self.artist_name = artist_name
        self.id = id
        self.score = score
        self.title = title

    @classmethod
    def from_json(cls, j):
        return cls(
            j['artist_id'], j['artist_name'], j['id'], j['score'], j['title']
        )

    def __getitem__(self, item):
        return self.__dict__[item]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __str__(self):
        if self.artist_name:
            return '{0} - {1}'.format(self.artist_name, self.title)
        return self.title
