# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.


# This is the user_version of the metadata SQLite database schema that the code
# expects.
_SCHEMA_VERSION = 10

import io
import os

try:
    from pysqlite2 import dbapi2 as sqlite3
except ImportError:
    import sqlite3

class UnsupportedVersionError(Exception):
    def __init__(self, expected_version, actual_version):
        super().__init__("Expected metadata database to have schema "
                         f"v{expected_version}, got v{actual_version} instead")
        self.__expected_version = expected_version
        self.__actual_version = actual_version

    def expected_version(self):
        return self.__expected_version

    def actual_version(self):
        return self.__actual_version


def _open_index_db(filename, mode):
    readonly = 'w' not in mode
    exists = os.path.exists(filename)
    if readonly and not exists:
        raise IOError(f"{filename} not found")

    else:
        # Note that we open the index.db file with isolation_level=None,
        # so that we have full control over the database locking.
        if readonly and hasattr(sqlite3, 'SQLITE_OPEN_READONLY'):
            db = sqlite3.connect(filename, isolation_level=None,
                                 flags=sqlite3.SQLITE_OPEN_READONLY)
        else:
            db = sqlite3.connect(filename, isolation_level=None)

    return db


def _open_metadata_db(filename, mode):
    readonly = 'w' not in mode
    exists = os.path.exists(filename)
    if readonly and not exists:
        raise IOError(f"{filename} not found")

    else:
        # TODO: Once we can use the uri API (e.g. python3, or with an adapted
        # pysqlite), then open the metadata db with nolock=1&immutable=1, to
        # prevent locking on the metadata db. Once written, it is never
        # modified. This might enhance performance by having fewer filesystem
        # operations.
        if readonly and hasattr(sqlite3, 'SQLITE_OPEN_READONLY'):
            db = sqlite3.connect(filename, flags=sqlite3.SQLITE_OPEN_READONLY)
        else:
            db = sqlite3.connect(filename)

        if exists:
            _validate_schema(db)

        return db


def _validate_schema(db):
    # db is an open sqlite3 connection to a metadata database.  Verify that
    # the schema for this database is one that we can handle.
    user_version = db.execute("PRAGMA user_version").fetchone()[0]
    if user_version != _SCHEMA_VERSION:
       raise UnsupportedVersionError(_SCHEMA_VERSION, user_version)
