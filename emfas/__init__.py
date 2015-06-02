from gevent import monkey
monkey.patch_all()

import livestreamer
import livestreamer.buffers

import collections
import tempfile
import gevent.pool
import gevent.queue
from emfas.codegen import codegen
from emfas.moomash import MoomashAPI


class Emfas(object):
    def __init__(self, api_key, queue_items=20):
        self.moomash = MoomashAPI(api_key)

        self._worker = None

        self._queue = collections.deque([], queue_items)

    @property
    def is_running(self):
        return self._worker is not None

    def start(self, segment_provider):
        self._worker = gevent.Greenlet(self._io_read, segment_provider)
        self._worker.start()

    def _io_read(self, segment_provider):
        for item in segment_provider:
            self._queue.append(item)
        self._worker = None

    def identify(self):
        with tempfile.NamedTemporaryFile() as fp:
            for segment in self._queue:
                fp.write(segment)
            fp.flush()

            code = codegen(fp.name)
            songs = self.moomash.identify(code)
            return songs


class TwitchSegmentProvider(collections.Iterator):
    CHUNK_SIZE = 1024*1024

    def __init__(self, url):
        self.ls = livestreamer.Livestreamer()

        streams = self.ls.streams(url)
        self._stream = streams.get('audio')
        if self._stream is None:
            self._stream = streams['worst']

        self._fd = self._stream.open()

    def __next__(self):
        # this depends on that every read returns exactly
        # one segment, which might fail sometimes
        chunk = self._fd.read(self.CHUNK_SIZE)
        if not chunk:
            raise StopIteration

        return chunk

    next = __next__


class CallbackRingBuffer(livestreamer.buffers.RingBuffer):
    def __init__(self, callback, *args, **kwargs):
        livestreamer.buffers.RingBuffer.__init__(self, *args, **kwargs)
        self.callback = callback

    def write(self, data):
        livestreamer.buffers.RingBuffer.write(self, data)
        self.callback(data)


class TwitchSegmentProvider2(collections.Iterator):
    def __init__(self, url):
        self.ls = livestreamer.Livestreamer()

        streams = self.ls.streams(url)
        self._stream = streams.get('audio')
        if self._stream is None:
            self._stream = streams['worst']

        self._fd = self._stream.open()

        self._segments = gevent.queue.Queue()
        size = self._fd.buffer.buffer_size
        self._fd.buffer = CallbackRingBuffer(self._segments.put, size=size)

    def __next__(self):
        segment = self._segments.get(block=True)
        if segment is None:
            raise StopIteration
        return segment

    next = __next__