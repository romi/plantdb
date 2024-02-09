#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Synchronize two ``FSDB`` databases.
"""
import argparse

from plantdb.sync import FSDBSync


def parsing():
    parser = argparse.ArgumentParser(description='Synchronize two FSDB databases.')
    parser.add_argument('origin', metavar='origin', type=str,
                        help='source database (path, local or remote)')
    parser.add_argument('target', metavar='target', type=str,
                        help='target database (path, local or remote)')
    return parser


def main():
    parser = parsing()
    args = parser.parse_args()
    fsdb_sync = FSDBSync(args.origin, args.target)
    fsdb_sync.sync()

if __name__ == '__main__':
    main()