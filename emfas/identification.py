from __future__ import unicode_literals

import json
import requests
import logging
import worker.server.lib.fp


logger = logging.getLogger('emfas')


class IdentificationException(Exception):
    pass


class MoomashAPIException(IdentificationException):
    def __init__(self, message, code, version):
        IdentificationException.__init__(self, message)

        self.message = message
        self.code = code
        self.version = version

    @classmethod
    def from_json(cls, j):
        return cls(
            j['message'], j['code'], j['version']
        )


class EchoprintServerAPIException(IdentificationException):
    pass


class IdentificationService(object):
    def identify(self, data, buffer_size):
        raise NotImplementedError


class MoomashAPI(IdentificationService):
    BASE_URL = 'http://api.mooma.sh/v1'
    HEADERS = {
        'Content-Type': 'application/octet-stream'
    }

    def __init__(self, api_key):
        self.api_key = api_key

    def identify(self, data, buffer_size):
        payload = json.dumps(data)

        url = '{0}/song/identify'.format(self.BASE_URL)
        response = requests.post(
            url, data=payload, headers=self.HEADERS,
            params=[('api_key', self.api_key)]
        )
        response.raise_for_status()

        j = response.json()
        logger.debug('Moomash response: {0!r}'.format(j))
        if not j['response']['status']['code'] == 0:
            raise MoomashAPIException.from_json(j['response']['status'])

        # sometimes songs looks like that...
        # {u'songs': [{}]}
        songs = [Song.from_json(song) for song in j['response']['songs'] if song]
        if len(songs) == 0:
            return None
        return songs[0]


class EchoprintServerAPI(IdentificationService):
    def __init__(self, **kwargs):
        self.fp = worker.server.lib.fp.FingerPrinter(**kwargs)

    def identify(self, data, buffer_size):
        response = self.fp.best_match_for_query(data['code'])
        logger.debug('EchoprintServer response, {0} in {1}ms'.format(
            response.message(), response.total_time)
        )
        if not response.match():
            return None

        metadata = response.metadata
        return Song(
            artist=metadata['artist'], title=metadata['track'],
            id=metadata['track_id'], source=metadata['source'],
            score=response.score
        )


class Song(object):
    def __init__(self, id=None, artist_id=None, artist=None, title=None, source=None, score=0):
        self.id = id
        self.artist_id = artist_id
        self.artist = artist
        self.title = title
        self.source = source
        self.score = score

    @classmethod
    def from_json(cls, j):
        return cls(
            id=j['id'], artist_id=j['artist_id'],
            artist=j['artist_name'], title=j['title'],
            score=j['score'], source=j.get('source')
        )

    def __getitem__(self, item):
        return self.__dict__[item]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __eq__(self, other):
        if isinstance(other, Song):
            return self.artist == other.artist and \
                   self.title == other.title

        return NotImplemented

    def __unicode__(self):
        if self.artist:
            return '{0} - {1}'.format(self.artist, self.title)
        return self.title

    def __str__(self):
        return unicode(self).encode('utf-8')

