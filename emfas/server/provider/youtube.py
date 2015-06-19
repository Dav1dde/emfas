from __future__ import unicode_literals
import base64
import logging

from emfas.server.provider import Provider
from urlparse import urlparse
import pafy
from emfas.server.song import extract_artist_title, Song
import emfas.codegen
import emfas.server.lib.fp


logger = logging.getLogger(__name__)


class YoutubeSong(Song):
    def __init__(self, item):
        self._p = item['pafy']
        self._p.fetch_basic()
        meta = item['playlist_meta']

        title, artist = extract_artist_title(meta['title'])
        if artist is None or title is None:
            raise ValueError('Unable to extract artist or title')

        # generate id from youtube id, to avoid duplicates
        track_id = 'YT{0}'.format(
            base64.b16encode(meta['encrypted_id'])
        )
        Song.__init__(
            self,
            track_id=track_id,
            artist=artist,
            track=title,
            length=meta['length_seconds'],
            source=meta['encrypted_id']
        )

        self._fp = None
        self._codever = None

    @property
    def fp(self):
        if self._fp is None:
            # start and end times on videos in playlists are history :(
            logger.debug('getting fingerprint for song')
            try:
                url = self._p.getbestaudio().url
            except (KeyError, IOError):
                url = self._p.getbest().url
            data = emfas.codegen.codegen_url(url)
            self._fp = emfas.server.lib.fp.decode_code_string(data['code'])
            self._codever = data['version']

        return self._fp

    @fp.setter
    def fp(self, value):
        self._fp = value

    @property
    def codever(self):
        return self._codever

    @codever.setter
    def codever(self, value):
        self._codever = value


class YoutubeProvider(Provider):
    def __init__(self, ns):
        Provider.__init__(self, ns)

    def responsible_for(self, uri):
        o = urlparse(uri)
        return (
            o.scheme in ('http', 'https') and
            'youtube' in o.netloc and
            'playlist' in o.path
        )

    def load(self, playlist):
        logger.info('parsing playlist: %s', playlist)
        p = pafy.get_playlist(playlist, basic=False)

        for i, item in enumerate(p['items'], start=1):
            logger.info('processing item %s/%s - %s',
                i, len(p['items']), item['playlist_meta']['title'])
            try:
                song = YoutubeSong(item)
            except Exception as e:
                logger.info('skipping because of "%s"', e)
            else:
                logger.debug('got song: %s', song)
                if song is not None:
                    yield song
