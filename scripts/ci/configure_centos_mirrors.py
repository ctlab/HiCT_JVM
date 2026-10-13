#!/usr/bin/env python
"""Add a second CentOS 7 archive URL without changing GPG verification.

Runs with the manylinux image's system Python 2.7 before installing build tools.
Yum still validates repository metadata checksums and CentOS package signatures.
"""
from __future__ import print_function
import glob
import io
import re

PATTERN = re.compile(r'^baseurl=https://vault\.centos\.org/centos/(?:\$releasever|7)/(.*)$')


def add_fallback(text):
    lines = text.splitlines()
    result = []
    for i, line in enumerate(lines):
        result.append(line)
        match = PATTERN.match(line)
        if not match:
            continue
        fallback = '        https://archive.kernel.org/centos-vault/7.9.2009/' + match.group(1)
        if i + 1 == len(lines) or lines[i + 1] != fallback:
            result.append(fallback)
    return '\n'.join(result) + '\n'


if __name__ == '__main__':
    for path in glob.glob('/etc/yum.repos.d/CentOS-*.repo'):
        with io.open(path, encoding='utf-8') as stream:
            original = stream.read()
        updated = add_fallback(original)
        if updated != original:
            with io.open(path, 'w', encoding='utf-8') as stream:
                stream.write(updated)
            print('Added signed-package archive fallback:', path)
