#!/usr/bin/env python3
"""Verify a copied toolchain without modifying its signed files."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'toolchains'))
from prepare_bundled_tools import verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--platform', required=True)
    args = parser.parse_args()
    verify(args.directory, args.platform)
    print('Verified copied toolchain checksums:', args.directory)


if __name__ == '__main__':
    main()
