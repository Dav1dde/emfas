from __future__ import unicode_literals

import json
import os
import echoprint
from gevent import subprocess
from io import BytesIO
import struct


def find_echoprint_codegen():
    # TODO improve
    # https://github.com/echonest/pyechonest/commit/764fd569245c9b03c72f149066e5bf5ccba97a6b
    ret = os.getenv('ECHOPRINT_CODEGEN')
    if ret is not None:
        return ret

    return 'echoprint-codegen'

_echoprint_codegen = find_echoprint_codegen()


def get_samples(io):
    while True:
        sample = io.read(2)
        if not sample:
            raise StopIteration
        yield struct.unpack('h', sample)[0] / 32768.0


def codegen(filename, start=-1, duration=-1, codegen_exe=None):
    if codegen_exe is None:
        codegen_exe = _echoprint_codegen

    args = [codegen_exe, filename]
    if start >= 0:
        args.append(str(start))
    if duration >= 0:
        args.append(str(duration))

    p = subprocess.Popen(args, universal_newlines=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    return json.loads(stdout)


def codegen_url(url, start=-1, duration=-1):
    args = [
        'ffmpeg',
        '-loglevel', 'quiet',
        '-i', url,
        '-ac', '1',
        '-ar', '11025',
        '-f', 's16le',
    ]
    if start > 0:
        args.extend(['-ss', str(start)])
    if duration > 0:
        args.extend(['-t', str(duration)])
    args.append('-')

    ffmpeg = subprocess.Popen(args, stdin=subprocess.PIPE,
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (stdout, stderr) = ffmpeg.communicate()

    io = BytesIO(stdout)
    return echoprint.codegen(get_samples(io), 0)


