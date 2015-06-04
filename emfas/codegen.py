from __future__ import unicode_literals

import json
import os
from gevent import subprocess


def find_echoprint_codegen():
    # TODO improve
    # https://github.com/echonest/pyechonest/commit/764fd569245c9b03c72f149066e5bf5ccba97a6b
    ret = os.getenv('ECHOPRINT_CODEGEN')
    if ret is not None:
        return ret

    return 'echoprint-codegen'

_echoprint_codegen = find_echoprint_codegen()


def codegen(filename, start=-1, duration=-1, codegen_exe=None):
    if codegen_exe is None:
        codegen_exe = _echoprint_codegen

    args = [codegen_exe, filename]
    if start >= 0:
        args.append(start)
    if duration >= 0:
        args.append(duration)

    p = subprocess.Popen(args, universal_newlines=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    return json.loads(stdout)


