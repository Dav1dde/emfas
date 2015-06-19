from __future__ import unicode_literals
import struct

from gevent import monkey
monkey.patch_all()

import livestreamer
import livestreamer.buffers

import collections
import logging
import tempfile
import gevent.queue
import gevent.pool
from gevent import subprocess
from itertools import islice

import echoprint
import emfas.codegen


Unit = collections.namedtuple('Unit', ['size', 'name'])

logger = logging.getLogger('emfas')


class EmfasException(Exception):
    pass


class BaseEmfas(object):
    UNIT = Unit(1, 'unit')

    def __init__(self, identification_service=None, buffer_size=20):
        self.identification_service = identification_service

        self._is_running = False
        self._worker_pool = gevent.pool.Group()
        self._queue = collections.deque([], buffer_size*self.UNIT.size)

    @property
    def is_running(self):
        return self._is_running

    def start(self, data_provider):
        self._queue = collections.deque([], self._queue.maxlen)
        self._worker_pool.kill()
        self._is_running = True
        let = self._worker_pool.spawn(self._io_read, data_provider)
        logger.info('Emfas worker started')

        return let

    def _io_read(self, data_provider):
        try:
            for item in data_provider:
                self._queue.append(item)
        finally:
            self._is_running = False
            data_provider.close()
            self._worker_pool.kill()
        logger.info('Emfas worker stopped')

    def identify(self, buffer_sizes=None, score=50, identification_service=None):
        """
        Identify the currently playing song

        :param buffer_sizes: A list of numbers of
        buffers sizes to try and identify. If any segment yields a song,
        with an acceptable score, the song will be returned immediately.
        :return: A list of Song objects returned by the identification service
        :rtype: emfas.identification.Song | None
        """
        if buffer_sizes is None:
            buffer_sizes = [None]

        ret_song = None
        for buffer_size in buffer_sizes:
            song = self.get_song_for_segment(
                buffer_size, identification_service=identification_service
            )
            if song is not None:
                logger.debug('Returned song %s, score: %s',
                             song, song.score)
                if song.score > score:
                    return song
                if ret_song is None or song.score > ret_song.score:
                    ret_song = song

        logger.info('Found song: %s', ret_song)
        # return the best found song or None
        return ret_song

    def get_song_for_segment(self, buffer_size, identification_service=None):
        if buffer_size is None:
            buffer_size = self._queue.maxlen

        if identification_service is None:
            identification_service = self.identification_service
        if identification_service is None:
            raise ValueError('No identification service available')

        code = self.get_echoprint(buffer_size)
        if code is None:
            return None

        return identification_service.identify(code, buffer_size)

    def get_echoprint(self, buffer_size=None):
        if buffer_size is None:
            buffer_size = self._queue.maxlen
        else:
            buffer_size *= self.UNIT.size

        # maxlen is on purpose
        start_index = max(0, self._queue.maxlen - buffer_size)

        logger.debug(
            '%s/%s %s available, using last %s %ss'.format(
                len(self._queue)/self.UNIT.size,
                self._queue.maxlen/self.UNIT.size,
                self.UNIT.name,
                buffer_size/self.UNIT.size,
                self.UNIT.name
            )
        )

        data = islice(self._queue, start_index, self._queue.maxlen)
        return self._get_echoprint(data)

    def _get_echoprint(self, data):
        raise NotImplementedError


class EmfasEchoprintExe(BaseEmfas):
    # this works with segments, since you can't possibly
    # know how long a segment is in seconds
    UNIT = Unit(1, 'segment')

    def _get_echoprint(self, data):
        with tempfile.NamedTemporaryFile() as fp:
            for datum in data:
                fp.write(datum)
            fp.flush()

            code = emfas.codegen.codegen(fp.name)
            if len(code) == 0 or 'error' in code[0]:
                return None
            return code[0]


class FFmpegEmfas(BaseEmfas):
    # the unit size is the sample rate
    UNIT = Unit(11025, 'second')

    def __init__(self, api_key, buffer_length=60):
        BaseEmfas.__init__(self, api_key, buffer_length)

        if echoprint is None:
            raise ImportError('Install the echoprint extension for '
                              'this emfas worker.')

    def _io_read(self, segment_provider):
        process = subprocess.Popen([
            'ffmpeg',
            '-loglevel', 'warning',
            '-i', '-',
            '-ac', '1',
            '-ar', '11025',
            '-f', 's16le',
            '-'
        ], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        def ffmpeg_processor():
            while True:
                sample = process.stdout.read(2)
                if not sample:
                    break
                data = struct.unpack('<h', sample)[0] / 32768.0
                self._queue.append(data)

        let = self._worker_pool.spawn(ffmpeg_processor)
        let.link(lambda g: process.terminate())

        try:
            for item in segment_provider:
                process.stdin.write(item)
        finally:
            self._is_running = False
            segment_provider.close()
            self._worker_pool.kill()

    def _get_echoprint(self, data):
        return echoprint.codegen(data, 0)


Emfas = FFmpegEmfas


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
        self._timeout = 20

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