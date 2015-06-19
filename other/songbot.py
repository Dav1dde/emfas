from __future__ import unicode_literals

import time
import gevent
import signal
import sys

from utopia.client import EasyClient, Identity
from utopia import signals

import logging
import argparse
import re
from emfas.worker import Emfas, TwitchSegmentProvider2, EmfasException
from emfas.identification import MoomashAPI, EchoprintServerAPI, IdentificationService

logger = logging.getLogger('songbot')

RATINGS = [(50, 'bad'), (100, 'ok'), (500, 'good'), (sys.maxint, 'amazing')]

_URL_REGEX = re.compile(r'(\w+://)[^\s]{2,}\s*\.\s*(?P<tld>\w+)',
                        re.IGNORECASE | re.MULTILINE)
_URL_REGEX2 = re.compile(r'\w+://', re.IGNORECASE)


def filter_urls(s, repl=''):
    s = _URL_REGEX.sub(repl, s)
    s = _URL_REGEX2.sub('', s)
    return s


def format_song(song, ratings=RATINGS):
    text = filter_urls(unicode(song))
    upper = sum(1 for c in text if c.isupper())
    if upper > len(text)/2:
        text = text.lower()

    rating = None
    for threshold, name in ratings:
        if song.score < threshold:
            rating = name
            break

    return 'Song: {0} | Accuracy: {1} ({2})' \
        .format(text, song.score, rating)


class SongBotException(Exception):
    pass


class EmfasNotRunning(SongBotException):
    pass


class NoSongFound(SongBotException):
    pass


class CombinedIdentificationService(IdentificationService):
    def __init__(self, *args):
        self.services = args

    def identify(self, code, buffer_size):
        for service, buffer_sizes in self.services:
            if buffer_sizes is not None and buffer_size not in buffer_sizes:
                continue

            song = service.identify(code, buffer_size)
            if song and song.score > 50:
                return song


class BaseSongBot(object):
    def __init__(self, ident, broadcaster, api_key):
        self.client = EasyClient(ident, 'irc.twitch.tv', port=6667)

        self.broadcaster = broadcaster
        self.channel = None

        self._restart_delay = 300
        self._rate_limit = 30
        self._last_fetch = (0, None)

        self._identify_sizes = [15, 30, 50, 70, 100, 120, 150]
        identification_service = CombinedIdentificationService(
            (EchoprintServerAPI(), None),
            (MoomashAPI(api_key), [50, 100,  150])
        )
        self.emfas = Emfas(identification_service, buffer_length=150)
        self._start_emfas()

        signals.on_registered.connect(self._join, sender=self.client)
        signals.m.on_PUBMSG.connect(self.on_pubmsg, sender=self.client)

    def connect(self):
        logger.info('Connecting as: \'{0}\''
                    .format(self.client.identity.nick))
        return self.client.connect()

    def send(self, text):
        self.client.privmsg(self.channel, text)

    def _join(self, *args, **kwargs):
        self.channel = '#{0}'.format(self.broadcaster)
        logger.info('Joining channel {0}'.format(self.channel))
        self.client.join_channel(self.channel)

    def _start_emfas(self):
        logger.debug('Starting emfas')

        url = 'twitch.tv/{}'.format(self.broadcaster)
        try:
            data_provider = TwitchSegmentProvider2(url)
        except EmfasException as e:
            logger.debug('Unable to create segment provider: {0}, '
                         'retrying in {1} seconds'
                         .format(e, self._restart_delay))
        except gevent.GreenletExit:
            raise
        except Exception:
            logger.warn('Unknown exception', exc_info=True)
        else:
            let = self.emfas.start(data_provider)
            let.link(lambda g: self._start_emfas())
            logger.info('Emfas started')
            return

        # start if an exception occurred
        gevent.spawn_later(self._restart_delay, self._start_emfas)

    def on_pubmsg(self, client, prefix, target, args):
        text = args[0].strip().lower()

        if text.startswith('!song'):
            logger.info('Handling song command')
            self.on_song()

    def on_song(self):
        since = time.time() - self._last_fetch[0]
        if since < self._rate_limit:
            last_song = self._last_fetch[1]
            logger.info('Rate limit in effect, last song: {0}'
                         .format(last_song))
            self.handle_rate_limited(since, last_song)
            return
        # stop gevent race conditions
        self._last_fetch = (time.time(), None)

        song = None
        try:
            song = self.get_song()
        except SongBotException as e:
            logger.debug(str(e))
            self.handle_song_error(e)
        else:
            logger.info('Returning song: \'{song}\', score: {song.score}'
                        .format(song=song))
            self.handle_song(song)

        # no need to worry about race conditions, set it properly
        self._last_fetch = (time.time(), song)

    def get_song(self):
        if not self.emfas.is_running:
            raise EmfasNotRunning('Emfas is not running')

        song = self.emfas.identify(buffer_sizes=self._identify_sizes)
        if song is None:
            raise NoSongFound('No song could be identified')

        return song

    def handle_rate_limited(self, time_since, last_message):
        raise NotImplementedError

    def handle_song(self, song):
        raise NotImplementedError

    def handle_song_error(self, exc):
        raise NotImplementedError


class SongBot(BaseSongBot):
    def __init__(self, ident, broadcaster, api_key):
        BaseSongBot.__init__(self, ident, broadcaster, api_key)

    def handle_rate_limited(self, time_since, last_song):
        if last_song is not None:
            self.send('Last {0}'.format(format_song(last_song)))

    def handle_song(self, song):
        self.send(format_song(song))

    def handle_song_error(self, exc):
        pass


def main():
    parser = argparse.ArgumentParser('songbot')
    parser.add_argument(
        '-c', '--channel', dest='channel',
        required=True, help='List of channels'
    )
    parser.add_argument('--api-key', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('username', help='Twitch username')
    parser.add_argument('password', help='Oauth password for twitch chat')
    ns = parser.parse_args()

    if ns.debug:
        logging.basicConfig(
            format='[%(asctime)s][%(levelname)s\t][%(name)-7s\t]: %(message)s',
            datefmt='%m/%d/%Y %H:%M:%S', level=logging.DEBUG
        )
        logging.getLogger('requests.packages.urllib3.connectionpool')\
            .setLevel(logging.WARNING)
        logging.getLogger('emfas.server.lib.fp')\
            .setLevel(logging.WARNING)

    ident = Identity(ns.username, password=ns.password)
    songbot = SongBot(ident, ns.channel, ns.api_key)

    def print_song(*args, **kwargs):
        try:
            print '[INTERRUPT] {0}'.format(songbot.get_song())
        except SongBotException as e:
            print '[INTERRUPT] {0!r}'.format(e)

    def print_song2(*args, **kwargs):
        if not songbot.emfas.is_running:
            print '[INTERRUPT] Emfas not running'
        else:
            print songbot.emfas.identify(None)

    gevent.signal(signal.SIGUSR1, print_song)
    gevent.signal(signal.SIGUSR2, print_song2)

    songbot.connect().get()
    songbot.client._io_workers.join()


if __name__ == '__main__':
    main()
