import argparse
import logging
import itertools
import datetime
import signal
from emfas.server.utils import (
    ijson, grouper, SimpleJSONArrayWriter, NullWriter, committing
)
from emfas.server import EchoprintServer
from emfas.server.song import Song
import emfas.server.lib.fp


def _ingest_dry_run(es, w, itr, check_duplicates=False):
    for song in itr:
        w.write(song.to_echoprint())


def _ingest(es, w, itr, check_duplicates=False):
    for song in itr:
        es.ingest(song, commit=False,
                  check_duplicates=check_duplicates)
        w.write(song.to_echoprint())


def ingest(ns):
    """
    Ingest all data from a datasource

    :param ns: Namespace object with required config
    :return: None
    """
    # import here so the other functions work even without the
    # echoprint extension installed
    import emfas.server.provider

    es = EchoprintServer(
        solr_url=ns.solr, tyrant_address=(ns.tyrant_host, ns.tyrant_port)
    )

    itr = None
    for provider in emfas.server.provider.provider:
        p = provider(ns)
        if p.responsible_for(ns.url):
            itr = p.load(ns.url)
            break

    if itr is None:
        raise ValueError('No provider found for URI')

    writer = NullWriter()
    if ns.dump:
        writer = SimpleJSONArrayWriter(ns.dump)

    func = _ingest
    if ns.dry_run:
        func = _ingest_dry_run

    with committing(es), writer as w:
        func(es, w, itr, check_duplicates=ns.check_duplicates)


def fastingest(ns):
    """
    A really fast ingest version, unlike the
    echoprint server utility, which will eat all of you RAM.

    :param ns: Namespace object with required config
    :return: None
    """
    es = EchoprintServer(
        solr_url=ns.solr, tyrant_address=(ns.tyrant_host, ns.tyrant_port)
    )
    import_date = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    status = 0
    start_time = datetime.datetime.utcnow()

    def signal_handler(signum, frame):
        diff = datetime.datetime.utcnow() - start_time
        print '{0}, {1}'.format(diff, status)
    signal.signal(signal.SIGUSR1, signal_handler)
    signal.siginterrupt(signal.SIGUSR1, False)

    with committing(es), open(ns.path) as f:
        data = ijson.items(f, 'item')

        for item in data:
            song = Song.from_echoprint(item, import_date=import_date)
            if song is not None:
                # don't commit, the contextmanager (with) takes care of it
                es.ingest(song, commit=False,
                          check_duplicates=ns.check_duplicates)
            status += 1


def split(ns):
    """
    Split a json file into multiple (a better version of
    echoprints splitdata.

    :param ns: Namespace object with required config
    :return: None
    """
    with open(ns.path) as f:
        data = ijson.items(f, 'item')
        counter = itertools.count(start=1)

        for i, group in enumerate(grouper(ns.num_items, data), start=1):
            out_path = '{0}.{1:03d}'.format(ns.path, next(counter))

            with SimpleJSONArrayWriter(out_path) as w:
                for item in group:
                    w.write(item)


_IGNORED_SIZE_EVENTS = ('end_map', 'end_array', 'map_key')

def size(ns):
    """
    Count the items of a json file (e.g. a echoprint dump)

    :param ns: Namespace object with required config
    :return: The size
    """
    s = 0
    with open(ns.path) as f:
        events = ijson.parse(f)

        for space, event, data in events:
            if space == 'item' and event not in _IGNORED_SIZE_EVENTS:
                s += 1

    return s


def main():
    parser = argparse.ArgumentParser('emfas.server')
    parser.add_argument('--solr', default='http://localhost:8502/solr/fp')
    parser.add_argument('--tyrant-host', default='localhost')
    parser.add_argument('--tyrant-port', type=int, default=1978)
    parser.add_argument('--verbose', action='store_true')

    subparsers = parser.add_subparsers(dest='subparser_name')
    ingest_parser = subparsers.add_parser('ingest')
    ingest_parser.add_argument('--check-duplicates', action='store_true')
    ingest_parser.add_argument('--dry-run', action='store_true')
    ingest_parser.add_argument('--dump')
    ingest_parser.add_argument('url')

    fastingest_parser = subparsers.add_parser('fastingest')
    fastingest_parser.add_argument('--check-duplicates', action='store_true')
    fastingest_parser.add_argument('path')

    split_parser = subparsers.add_parser('split')
    split_parser.add_argument('num_items', type=int)
    split_parser.add_argument('path')

    size_parser = subparsers.add_parser('size')
    size_parser.add_argument('path')

    ns = parser.parse_args()

    if ns.verbose:
        logging.basicConfig(
            format='[%(asctime)s][%(levelname)s\t][%(name)-7s\t]: %(message)s',
            datefmt='%m/%d/%Y %H:%M:%S', level=logging.INFO
        )

    commands = {
        'ingest': ingest, 'fastingest': fastingest,
        'split': split, 'size': size
    }

    logging.getLogger(__name__).info('Arguments: {0}'.format(ns))
    ret = commands[ns.subparser_name](ns)
    if ret is not None:
        print ret


if __name__ == '__main__':
    main()