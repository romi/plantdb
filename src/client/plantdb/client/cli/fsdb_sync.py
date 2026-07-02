#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB Database Synchronizer

A simple command‑line utility that synchronizes two *FSDB* databases—whether they are local paths or remote locations.
It streamlines keeping data consistent across environments, which is essential for collaborative projects and backup workflows.

## Key Features

- Parses source (`origin`) and destination (`target`) arguments from the command line.
- Instantiates `FSDBSync` from the *plantdb* library to perform the synchronization logic.
- Executes the synchronization process with a single command, handling both local and remote databases.

## Usage Examples

### Synchronize a local database with a remote one
```shell
python fsdb_sync.py /path/to/origin.db user@remote:/path/to/target.db
```

### Synchronize two local databases
```shell
python fsdb_sync.py ./origin.db ./target.db
```
"""

import argparse

from plantdb.client.sync import FSDBSync


def parsing():
    parser = argparse.ArgumentParser(description="Synchronize two FSDB databases.")
    parser.add_argument(
        "origin",
        metavar="origin",
        type=str,
        help="source database (path, local or remote)",
    )
    parser.add_argument(
        "target",
        metavar="target",
        type=str,
        help="target database (path, local or remote)",
    )
    return parser


def main():
    parser = parsing()
    args = parser.parse_args()
    fsdb_sync = FSDBSync(args.origin, args.target)
    fsdb_sync.sync()


if __name__ == "__main__":
    main()
