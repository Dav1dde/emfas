import logging
from emfas.server.lib.fp import FingerPrinter


logger = logging.getLogger(__name__)


class EchoprintServer(object):
    def __init__(self, **kwargs):
        self.fp = FingerPrinter(**kwargs)

    def commit(self):
        self.fp.commit()

    def ingest(self, song, check_duplicates=False, commit=True):
        logger.info('ingesting song: {0}'.format(song))
        if check_duplicates and self.fp.metadata_for_track_id(song.track_id):
            logger.info(' --> song already in database, skipping')
            return False

        data = song.to_dict()
        self.fp.ingest(data, do_commit=commit)
        return True

    def ingest_many(self, songs, commit=True):
        try:
            for song in songs:
                self.ingest(song, commit=False)
        finally:
            if commit:
                self.fp.commit()
