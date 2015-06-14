import logging
from emfas.server.lib.fp import FingerPrinter


logger = logging.getLogger(__name__)


class EchoprintServer(object):
    def __init__(self, **kwargs):
        self.fp = FingerPrinter(**kwargs)

    def ingest(self, songs):
        try:
            for song in songs:
                logger.info('ingesting song: {0}'.format(song))
                if self.fp.metadata_for_track_id(song.track_id):
                    logger.info(' --> song already in database, skipping')
                    continue

                data = song.to_dict()
                self.fp.ingest(data, do_commit=False)
        finally:
            self.fp.commit()
