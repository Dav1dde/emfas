from __future__ import unicode_literals

import argparse
import gevent
from emfas import Emfas, TwitchSegmentProvider


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', required=True, help='moomash api key')
    parser.add_argument('url', help='twitch url')
    ns = parser.parse_args()

    emfas = Emfas(ns.api_key)
    emfas.start(TwitchSegmentProvider(ns.url))

    def every_minute():
        songs = emfas.identify()
        print 'Songs:', ', '.join(map(unicode, songs))

        gevent.spawn_later(60, every_minute)
    gevent.spawn_later(60, every_minute)

    gevent.wait([emfas._worker])


if __name__ == '__main__':
    main()