import argparse
import logging
import itertools
import datetime
from emfas.server import EchoprintServer
from emfas.server.song import Song
import emfas.server.provider
import emfas.server.lib.fp


def ingest(ns):
    """
    Ingest all data from a datasource

    :param ns: Namespace object with required config
    :return: None
    """
    from emfas.server.utils import SimpleJSONArrayWriter, NullWriter, committing

    es = EchoprintServer(solr_url=ns.solr, tyrant_address=(ns.tyrant_host, ns.tyrant_port))

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

    with committing(es), writer as w:
        for song in itr:
            added = True
            if not ns.dry_run:
                added = es.ingest(song, commit=False)

            if added:
                w.write(song.to_echoprint())


def fastingest(ns):
    """
    A really fast ingest version, unlike the
    echoprint server utility, which will eat all of you RAM.

    :param ns: Namespace object with required config
    :return:
    """
    from emfas.server.utils import ijson, committing

    es = EchoprintServer(solr_url=ns.solr, tyrant_address=(ns.tyrant_host, ns.tyrant_port))
    import_date = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    with committing(es), open(ns.path) as f:
        data = ijson.items(f, 'item')

        for item in data:
            # see if there is an already decoded string
            decoded_code = item.get('fp')

            # if there is an encoded string, use that one
            code = item.get('code')
            if code is not None:
                decoded_code = emfas.server.lib.fp.decode_code_string(code)

            # neither decoded or encoded, continue
            if decoded_code is None:
                continue

            metadata = item['metadata']
            if 'track_id' not in metadata:
                metadata['track_id'] = emfas.server.lib.fp.new_track_id()

            song = Song(
                track_id=metadata['track_id'], fp=decoded_code,
                artist=metadata.get('artist'), release=metadata.get('release'),
                track=metadata.get('title'), length=metadata['duration'],
                codever=metadata['version'], source=metadata.get('source'),
                import_date=import_date
            )

            # don't commit, the contextmanager (with) takes care of it
            es.ingest(song, commit=False)


def split(ns):
    """
    Split a json file into multiple (a better version of
    echoprints splitdata.

    :param ns: Namespace object with required config
    :return: None
    """
    from emfas.server.utils import ijson, grouper, SimpleJSONArrayWriter

    with open(ns.path) as f:
        data = ijson.items(f, 'item')
        counter = itertools.count(start=1)

        for i, group in enumerate(grouper(ns.num_items, data), start=1):
            out_path = '{0}.{1:03d}'.format(ns.path, next(counter))

            with SimpleJSONArrayWriter(out_path) as w:
                for item in group:
                    w.write(item)


def main():
    parser = argparse.ArgumentParser('emfas.server')
    parser.add_argument('--solr', default='http://localhost:8502/solr/fp')
    parser.add_argument('--tyrant-host', default='localhost')
    parser.add_argument('--tyrant-port', type=int, default=1978)
    parser.add_argument('--verbose', action='store_true')

    subparsers = parser.add_subparsers(dest='subparser_name')
    ingest_parser = subparsers.add_parser('ingest')
    ingest_parser.add_argument('url')
    ingest_parser.add_argument('--dump')
    ingest_parser.add_argument('--dry-run', action='store_true')

    fastingest_parser = subparsers.add_parser('fastingest')
    fastingest_parser.add_argument('path')

    split_parser = subparsers.add_parser('split')
    split_parser.add_argument('num_items', type=int)
    split_parser.add_argument('path')

    ns = parser.parse_args()

    if ns.verbose:
        logging.basicConfig(
            format='[%(asctime)s][%(levelname)s\t][%(name)-7s\t]: %(message)s',
            datefmt='%m/%d/%Y %H:%M:%S', level=logging.INFO
        )

    commands = {
        'ingest': ingest,
        'fastingest': fastingest,
        'split': split
    }

    commands[ns.subparser_name](ns)


if __name__ == '__main__':
    main()