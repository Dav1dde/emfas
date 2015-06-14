from __future__ import unicode_literals
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
            'codever': self.codever,
        }

        for name, value in [('artist', self.artist), ('release', self.release),
                            ('track', self.track), ('source', self.source),
                            ('import_date', self.import_date)]:
            if value is not None:
                data[name] = value

        return data

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return '{self.track_id} | ' \
               '{self.artist} - {self.track}'.format(self=self)