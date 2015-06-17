from contextlib import contextmanager
import itertools
import warnings
import os
import json

try:
    from emfas.server.lib.other import ijsoncffi as ijson
except ImportError:
    try:
        from ijson.backends import yajl2 as ijson
    except ImportError:
        try:
            from ijson.backends import yajl as ijson
        except ImportError:
            from ijson.backends import python as ijson
            warnings.warn(
                'Using python implementation, this will be slow! '
                'Install yajl2 and cffi to make it faster.'
            )

def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


def add_if(d, key, value, cond=bool):
    if cond(value):
        d[key] = value


class SimpleJSONArrayWriter(object):
    def __init__(self, path):
        self.path = path
        self._file = None

    def write(self, obj):
        self._file.write(json.dumps(obj))
        self._file.write(',')

    def __enter__(self):
        self._file = open(self.path, 'wb')
        self._file.write('[')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file is None:
            return

        if self._file.tell() > 1:
            # we wrote at least one object, so there is a
            # surplus comma, overwrite it
            self._file.seek(-1, os.SEEK_CUR)
        self._file.write(']')
        self._file.close()
        self._file = None


class NullWriter(object):
    def __init__(self, *args, **kwargs):
        pass

    def write(self, value):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@contextmanager
def committing(thing):
    try:
        yield thing
    finally:
        thing.commit()