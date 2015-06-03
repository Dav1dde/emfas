from gevent import monkey
monkey.patch_all()

import livestreamer
import livestreamer.buffers

import collections
import logging
import tempfile
import gevent.queue
import gevent.pool
from emfas.codegen import codegen
from emfas.moomash import MoomashAPI


logger = logging.getLogger('emfas')


class EmfasException(Exception):
    pass


class Emfas(object):
    def __init__(self, api_key, queue_items=20):
        self.moomash = MoomashAPI(api_key)

        self._worker = None
        self._queue = collections.deque([], queue_items)

    @property
    def is_running(self):
        return self._worker is not None

    def start(self, segment_provider):
        self._queue = collections.deque([], self._queue.maxlen)
        if self._worker is not None:
            self._worker.kill(block=False)
        self._worker = gevent.Greenlet(self._io_read, segment_provider)
        self._worker.start()
        logger.info('Emfas worker started')

    def _io_read(self, segment_provider):
        try:
            for item in segment_provider:
                self._queue.append(item)
        finally:
            self._worker = None
            segment_provider.close()
        logger.info('Emfas worker stopped')

    def identify(self):
        logger.debug('{0}/{1} segments available'.format(
                     len(self._queue), self._queue.maxlen))
        with tempfile.NamedTemporaryFile() as fp:
            for segment in self._queue:
                fp.write(segment)
            fp.flush()

            code = codegen(fp.name)
            songs = self.moomash.identify(code)
            return songs


class CallbackRingBuffer(livestreamer.buffers.RingBuffer):
    def __init__(self, callback, *args, **kwargs):
        livestreamer.buffers.RingBuffer.__init__(self, *args, **kwargs)
        self.callback = callback

    def write(self, data):
        livestreamer.buffers.RingBuffer.write(self, data)
        self.callback(data)

    def close(self):
        livestreamer.buffers.RingBuffer.close(self)
        self.callback(None)


class TwitchSegmentProvider(collections.Iterator):
    def __init__(self, url):
        self.ls = livestreamer.Livestreamer()

        streams = self.ls.streams(url)
        self._stream = streams.get('audio')
        if self._stream is None:
            try:
                self._stream = streams['worst']
            except KeyError:
                raise EmfasException('Unable to find stream, offline?')

        self._fd = self._stream.open()
        self._timeout = 15

        self._segments = gevent.queue.Queue()
        size = self._fd.buffer.buffer_size
        self._fd.buffer = CallbackRingBuffer(self._segments.put, size=size)

    def __next__(self):
        try:
            segment = self._segments.get(timeout=self._timeout)
        except gevent.queue.Empty:
            logger.warn('Did not receive a new segment!')
            raise StopIteration

        if segment is None:
            raise StopIteration
        return segment

    def close(self):
        logger.info('Closing TwitchSegmentProvider')

        self._segments.put(None)
        self._fd.close()

    next = __next__

# compatibility
TwitchSegmentProvider2 = TwitchSegmentProvider