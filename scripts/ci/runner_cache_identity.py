#!/usr/bin/env python3
"""Expose the hosted runner image identity for compiler/runtime cache keys."""
import os
import platform
from pathlib import Path

identity = '-'.join((os.environ.get('ImageOS', platform.system()),
                     os.environ.get('ImageVersion', platform.release()), platform.machine()))
with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
    output.write('identity=' + identity + '\n')
