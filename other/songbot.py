from __future__ import unicode_literals
import time
import gevent

from utopia.client import EasyClient, Identity
from utopia import signals

import logging
import argparse
import re
from emfas import Emfas, TwitchSegmentProvider2, EmfasException


_URL_REGEX = re.compile(r'(\w+://)?[^\s]{2,}\s*\.\s*(?P<tld>\w+)',
                        re.IGNORECASE | re.MULTILINE)
_URL_REGEX2 = re.compile(r'\w+://', re.IGNORECASE)
def filter_urls(s, repl=''):
    s = _URL_REGEX.sub(repl, s)
    s = _URL_REGEX2.sub('', s)
    return s


logger = logging.getLogger('songbots')


class SongBot(object):
    def __init__(self, ident, broadcaster, api_key):
        self.client = EasyClient(ident, 'irc.twitch.tv', port=6667)

        self._restart_delay = 300
        self._rate_limit = 30
        self._last_fetch = 0

        self.broadcaster = broadcaster
        self.emfas = Emfas(api_key, queue_items=25)
        self.sp = None
        self._start_emfas()

        signals.on_registered.connect(self._join, sender=self.client)
        signals.m.on_PUBMSG.connect(self.on_pubmsg, sender=self.client)

    def _join(self, *args, **kwargs):
        logger.info('Joining channel #{0}'.format(self.broadcaster))
        self.client.join_channel('#{0}'.format(self.broadcaster))

    def _start_emfas(self):
        logger.debug('Starting emfas')

        url = 'twitch.tv/{}'.format(self.broadcaster)
        try:
            self.sp = TwitchSegmentProvider2(url)
        except EmfasException as e:
            logger.debug('Unable to create segment provider: {0}, '
                         'retrying in {1} seconds'
                         .format(e, self._restart_delay))
            gevent.spawn_later(self._restart_delay, self._start_emfas)
            return

        self.emfas.start(self.sp)

        def link(greenlet):
            logger.info('Emfas worker finished, restarting')
            self._start_emfas()

        self.emfas._worker.link(link)
        logger.info('Emfas started')

    def connect(self):
        logger.info('Connecting as: \'{0}\''
                    .format(self.client.identity.nick))
        return self.client.connect()

    def on_pubmsg(self, client, prefix, target, args):
        text = args[0].strip().lower()

        if text.startswith('!song'):
            self.send_song(target)

    def send_song(self, target):
        logger.debug('Found song command')

        if not self.emfas.is_running:
            logger.debug('No emfas running')
            return

        now = time.time()
        if now - self._last_fetch < self._rate_limit:
            logger.debug('Rate limit in effect')
            return
        self._last_fetch = time.time()

        songs = self.emfas.identify()
        text = 'Song: Sorry I am unable to detect the current song'
        if len(songs) > 0:
            song = filter_urls(str(songs[0]))
            upper = sum(1 for c in song if c.isupper())
            if upper > len(song)/2:
                song = song.lower()
            text = 'Song: {0} | Score: {1}'\
                .format(song, songs[0].score)

        logger.debug('Sending: \'{0}\''.format(text))
        self.client.privmsg(target, text)


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
            format='[%(asctime)s] %(levelname)s:\t%(message)s',
            datefmt='%m/%d/%Y %H:%M:%S', level=logging.DEBUG
        )
        logging.getLogger('requests.packages.urllib3.connectionpool')\
            .setLevel(logging.WARNING)

    ident = Identity(ns.username, password=ns.password)
    songbot = SongBot(ident, ns.channel, ns.api_key)

    songbot.connect().get()
    songbot.client._io_workers.join()


if __name__ == '__main__':
    main()
