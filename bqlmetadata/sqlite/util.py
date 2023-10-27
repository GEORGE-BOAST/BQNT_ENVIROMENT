#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os
import errno

if os.name != "nt":
    # Stub out this Windows exception when we're not on Windows.
    class WindowsError(object):
        pass


def _makedirs(path):
    """
    Like os.makedirs(), but do not throw an exception if the path already
    exists.
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    except WindowsError as e:
        if e.winerror != 183: # Cannot create a file when that file already exists
            raise


def _unlink(filename):
    """Delete the file with the given name.

    However, don't raise an exception if the file does not exist.
    """
    try:
        os.unlink(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    except WindowsError as e:
        if e.winerror != 2:
            raise


def _recover_pending_entry(id_, filename, cursor):
    """Cleanup a pending metadata update in index.db.

    This should be called when a pending entry exists in index.db for which
    the process who created it does no longer exist. It attempts to delete
    a partly written metadata database, if any, and deletes the entry from
    the database.
    """
    try:
        _unlink(filename)
    except Exception as e:
        # we couldn't clean up the file. Keep the entry in the
        # database so that we try again to delete the file next time we
        # access the database. Reset its pending flag because there
        # can only ever be one pending row in the database, and we
        # might be going to add a new one.
        cursor.execute("UPDATE Metadata SET pending=NULL, update_time='1970-01-01 00:00:00' WHERE id = ?", (id_,))
    else:
        cursor.execute("DELETE FROM Metadata WHERE id = ?", (id_,))
