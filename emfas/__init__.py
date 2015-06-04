from __future__ import unicode_literals

from gevent import monkey
monkey.patch_all()

import livestreamer
import livestreamer.buffers

import collections
import logging
import tempfile
import gevent.queue
import gevent.pool
from itertools import islice
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

    def identify(self, segments=None, score=50):
        """
        Identify the currently playing song

        :param segments: A list of number of
        segments to try and identify. If any segment yields a song,
        with an acceptable score, the song will be returned immediately.
        :return: A list of moomash.Song objects
        :rtype: emfas.moomash.Song | None
        """
        if segments is None:
            segments = [None]

        ret_song = None
        for segment in segments:
            song = self.get_song_for_segment(segment)
            if song is not None:
                logger.debug('Returned song {0}, score: {1}'
                             .format(song, song.score))
                if song.score > score:
                    return song
                if ret_song is None or song.score > ret_song.score:
                    ret_song = song

        logger.info('Found song: {0}'.format(ret_song))
        # return the best found song or None
        return ret_song

    def get_song_for_segment(self, segment):
        code = self.get_echoprint(segment)
        if code is None:
            return None

        songs = self.moomash.identify(code)
        if len(songs) == 0:
            return None
        return songs[0]

    def get_echoprint(self, segments=None):
        if segments is None:
            segments = self._queue.maxlen
        # maxlen is on purpose
        start_index = max(0, self._queue.maxlen - segments)

        logger.debug('{0}/{1} segments available, using last {2} segments'
                     .format(len(self._queue), self._queue.maxlen, segments))

        with tempfile.NamedTemporaryFile() as fp:
            for segment in islice(self._queue, start_index, self._queue.maxlen):
                fp.write(segment)
            fp.flush()

            code = codegen(fp.name)
            if len(code) == 0 or 'error' in code[0]:
                return None
            return code[0]


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