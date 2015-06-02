from __future__ import unicode_literals

import json
from gevent import subprocess


_echoprint_codegen = 'echoprint-codegen'


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
