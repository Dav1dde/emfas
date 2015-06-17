from __future__ import unicode_literals
import emfas.server.lib.fp
import datetime
import re


TITLE_REGEX = [
    re.compile('(?P<artist>[^-]*)\s*-\s*(?P<title>.*)')
]

def extract_artist_title(s):
    for r in TITLE_REGEX:
        m = r.match(s)
        if m:
            return m.group('title'), m.group('artist')

    return s, None


class Song(object):
    def __init__(self, track_id=None, fp=None, artist=None, release=None,
                 track=None, length=None, codever=None, source=None, import_date=None):
        self.track_id = track_id
        self.fp = fp
        self.artist = artist
        self.release = release
        self.track = track
        self.length = length
        self.codever = codever
        self.source = source
        self.import_date = import_date

    def to_dict(self):
        if self.track_id is None:
            raise ValueError('TrackId required')
        if self.fp is None:
            raise ValueError('No fingerprint')
        if self.codever is None:
            raise ValueError('No echoprint-version')
        if self.length is None:
            raise ValueError('Length is required')

        data = {
            'track_id': self.track_id,
            'fp': self.fp,
            'length': self.length,
            'codever': '{0:.2f}'.format(float(self.codever)),
        }

        for name, value in [('artist', self.artist), ('release', self.release),
                            ('track', self.track), ('source', self.source),
                            ('import_date', self.import_date)]:
            if value is not None:
                data[name] = value

        return data

    def to_echoprint(self):
        return {
            'code': emfas.server.lib.fp.encode_code_string(self.fp),
            'metadata': {
                'track_id': self.track_id,
                'artist': self.artist,
                'title': self.track,
                'release': self.release,
                'version': self.codever,
                'duration': self.length,
                'source': self.source
            }
        }

    @classmethod
    def from_echoprint(cls, item, import_date=None):
        if import_date is None:
            import_date = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # see if there is an already decoded string
        decoded_code = item.get('fp')

        # if there is an encoded string, use that one
        code = item.get('code')
        if code is not None:
            decoded_code = emfas.server.lib.fp.decode_code_string(code)

        # neither decoded or encoded, continue
        if decoded_code is None:
            return

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

        return song

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return '{self.track_id} | ' \
               '{self.artist} - {self.track}'.format(self=self)