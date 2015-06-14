import argparse
import logging
from emfas.server import EchoprintServer
import emfas.server.provider


def ingest(ns):
    es = EchoprintServer(solr_url=ns.solr, tyrant_address=(ns.tyrant_host, ns.tyrant_port))

    url = ns.url
    for provider in emfas.server.provider.provider:
        p = provider(ns)
        if p.responsible_for(url):
            itr = p.load(url)
            es.ingest(itr)
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--solr', default='http://localhost:8502/solr/fp')
    parser.add_argument('--tyrant-host', default='localhost')
    parser.add_argument('--tyrant-port', type=int, default=1978)
    parser.add_argument('--verbose', action='store_true')

    subparsers = parser.add_subparsers(dest='subparser_name')
    ingest_parser = subparsers.add_parser('ingest')
    ingest_parser.add_argument('url')

    ns = parser.parse_args()

    if ns.verbose:
        logging.basicConfig(
            format='[%(asctime)s][%(levelname)s\t][%(name)-7s\t]: %(message)s',
            datefmt='%m/%d/%Y %H:%M:%S', level=logging.INFO
        )

    if ns.subparser_name == 'ingest':
        ingest(ns)


if __name__ == '__main__':
    main()