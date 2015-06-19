from __future__ import unicode_literals

import argparse
import logging
import gevent
import sys

from worker import Emfas, TwitchSegmentProvider2


class LevelFilter(logging.Filter):
    def __init__(self, levels):
        logging.Filter.__init__(self)

        self.levels = levels

    def filter(self, rec):
        return rec.levelno in self.levels


def setup_logging():
    root_logger = logging.getLogger()
    root_logger.handlers = []

    fmtr = logging.Formatter(
        '[%(asctime)s] %(name)s: %(levelname)s:\t%(message)s', '%m/%d/%Y %H:%M:%S'
    )

    stdout = logging.StreamHandler(sys.stdout)
    low_filter = LevelFilter((logging.DEBUG, logging.INFO))
    stdout.addFilter(low_filter)
    stdout.setFormatter(fmtr)
    root_logger.addHandler(stdout)

    stderr = logging.StreamHandler(sys.stderr)
    high_filter = LevelFilter(
        (logging.WARNING, logging.ERROR, logging.CRITICAL)
    )
    stderr.addFilter(high_filter)
    stderr.setFormatter(fmtr)
    root_logger.addHandler(stderr)

    root_logger.setLevel(logging.DEBUG)


def main():
    parser = argparse.ArgumentParser('emfas')
    parser.add_argument('--api-key', required=True, help='moomash api key')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('url', help='twitch url')
    ns = parser.parse_args()

    emfas = Emfas(ns.api_key)
    sp = TwitchSegmentProvider2(ns.url)
    emfas.start(sp)

    if ns.verbose > 0:
        setup_logging()
        if ns.verbose < 1:
            logging.getLogger('requests').setLevel(logging.WARNING)
        if ns.verbose < 2:
            sp.ls.set_loglevel(logging.WARNING)
        if ns.verbose < 3:
            logging.getLogger('requests.packages.urllib3.connectionpool')\
                .setLevel(logging.WARNING)

    def every_minute():
        songs = emfas.identify()
        print 'Songs:', ', '.join(map(unicode, songs))

        gevent.spawn_later(60, every_minute)
    gevent.spawn_later(60, every_minute)

    gevent.wait([emfas._worker])


if __name__ == '__main__':
    main()