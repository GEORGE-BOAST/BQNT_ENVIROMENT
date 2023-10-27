#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os
import sys
import logging
import datetime
import calendar
import random
import itertools
import sqlite3
from contextlib import contextmanager

from .database import (UnsupportedVersionError,
                       _open_index_db, _open_metadata_db)
from .serializer import SqliteMetadataSerializer
from .util import _unlink, _makedirs, _recover_pending_entry
from .version_util import decode_version
from ..metadata_serializer import MetadataDeserializer

from bqutil import ScopedTracer

_this_path = os.path.abspath(os.path.dirname(__file__))

_logger = logging.getLogger(__name__)


def _initialize_db(db):
    """Initialize the database in case we just created it.

    Start an IMMEDIATE transaction. There can only ever be one IMMEDIATE
    transaction going on on a database at the same time. We use
    this to protect ourselves from race conditions while initializing
    the database with the correct schema.
    """
    cursor = db.cursor()
    cursor.execute('BEGIN IMMEDIATE')
    cursor.execute('PRAGMA user_version')
    user_version = cursor.fetchone()[0]

    try:
        if user_version == 0:
            # initialize the index database
            schema_path = os.path.join(_this_path, 'index_schema.sql')
            with open(schema_path, 'r') as schema:
                cursor.executescript(schema.read())
    finally:
        # Note that executescript performs a COMMIT
        if user_version != 0:
            cursor.execute('COMMIT')


def _cleanup_db(db, path):
    """Attempt to delete superseeded metadata entries."""

    _logger.info("Cleaning up old metadata entries in db path: %s", path)

    cursor = db.cursor()

    cursor.execute('BEGIN IMMEDIATE')
    try:
        # Sort by ID as second criterion, so that if there are two entries with
        # the same update_time, the one with higher ID is taken as more recent.
        # This does typically not matter in practice, but avoids having to have
        # artifical one-second waits in unit tests.
        cursor.execute('SELECT id, filename, update_time FROM Metadata '
                       'WHERE pending IS NULL '
                       'ORDER BY update_time DESC, id DESC')
        metadata = cursor.fetchall()

        # Walk through the databases starting with the most recent one, and
        # keep the first valid one we find.
        while metadata:
            md = metadata[0]
            try:
                db_path_name = os.path.join(path, md[1])
                metadata_db = _open_metadata_db(db_path_name, 'r')
                # If we've made it this far, then the database is valid.
                metadata_db.close()
                break
            except (UnsupportedVersionError, IOError):
                # This database isn't valid, so remove it from the index
                # and try the next one.
                _logger.info(
                    "Removing invalid metadata db from index id=%s", md[0])
                cursor.execute('DELETE FROM Metadata WHERE id==?', (md[0],))
                metadata = metadata[1:]
    finally:
        # commit transaction to release lock
        cursor.execute('COMMIT')

    # Attempt to delete old (outdated) metadata files.  They are the ones
    # that remain in the list after discarding invalid databases earlier.
    if metadata:
        _logger.info("Removing old metadata files")
        for md in metadata[1:]:
            cursor.execute('BEGIN')
            # Ignore errors. We might not be able to delete the file
            # for various reasons, such as it being still in use.
            try:
                _logger.info("Deleting id %s", md[0])
                cursor.execute('DELETE FROM Metadata WHERE id==?', (md[0],))
                _unlink(os.path.join(path, md[1]))
            except Exception as ex:
                # file could not be deleted: rollback database transaction
                _logger.info("Failed to delete id %s", md[0])
                cursor.execute('ROLLBACK')
            else:
                # file could be deleted: commit database transaction
                _logger.info("Deleted id %s", md[0])
                cursor.execute('COMMIT')

    _logger.info("Done cleaning up db.")


@contextmanager
def _sqlite_row_factory(db, row_factory):
    prev_factory = db.row_factory
    try:
        db.row_factory = row_factory
        yield db.row_factory
    finally:
        db.row_factory = prev_factory


def _get_metadata_version_info(db):
    with _sqlite_row_factory(db, sqlite3.Row):
        cursor = db.cursor()
        results = cursor.execute(
            'SELECT name, value FROM MetadataProperties '
            'WHERE name in ("version", "metadata_build_version")')

        prev_version = None
        prev_metadata_build_version = None

        for row in results:
            if row['name'] == 'version':
                version_value = row['value']
                if version_value is not None:
                    prev_version = decode_version(
                        int(version_value[0]))
                    _logger.info("Previous version: %s", prev_version)
            elif row['name'] == 'metadata_build_version':
                prev_metadata_build_version = row['value']
                _logger.info("Previous metadata build version: %s",
                             prev_metadata_build_version)
            else:
                continue

        return prev_version, prev_metadata_build_version


def _update_metadata_cache_with_index_db(deserializer, path, index_db,
                                         mclient, mserver):
    """Update the matadata database.

    This method updates the metadata cache by consuming metadata from the given
    deserializer. It takes care of proper synchronization between multiple
    processes that try to update the metadata at the same time.

    This is the entry point for the child process that actually performs
    the update.
    """

    _logger.info("Initializing db..")

    _initialize_db(index_db)

    cursor = index_db.cursor()

    # First, see if there is another process running, by checking whether
    # there is a pending entry inside index.db. Note that we could have done
    # that already before this process was spawned, however between that check
    # and this process actually arriving at this point, another updater
    # process might have created a pending entry. If that happens, just wait
    # for the other process to finish and then exit ourselves without doing
    # anything.
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    run_update = True
    with mclient:
        cursor.execute('BEGIN IMMEDIATE')
        try:
            _logger.info("Checking for other running update processes...")
            cursor.execute('SELECT id, filename, update_time, '
                           'writer_pid, error_message FROM Metadata '
                           'WHERE pending IS NOT NULL')
            result = cursor.fetchone()
            id_, filename, update_time, writer_pid, error_message = (
                None, None, None, None, None)
            if result:
                id_, filename, update_time, writer_pid, error_message = result

                _logger.info(
                    "Found another running process id=%s "
                    "filename=%s update_time=%s pid=%s error=%s",
                    id_, filename, update_time, writer_pid, error_message)

                # Another process seems to be running. Note that the other
                # process cannot have exited between the time we queried the
                # database and now because we hold a lock on the database, and
                # the process would try to delete its pending row from the
                # database before exiting.

                try:
                    dt = datetime.datetime.strptime(
                        update_time, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    dt = datetime.datetime.strptime(
                        update_time, '%Y-%m-%d %H:%M:%S')

                timestamp = calendar.timegm(dt.utctimetuple())

                # Find out if another process actually exists, or whether
                # this is just a stale entry in the database.
                stale_entry = error_message is not None
                if not stale_entry:
                    stale_entry = not mclient.open(writer_pid, timestamp)

                if stale_entry:
                    # The other process does not exist anymore, or it failed
                    # previously and left its error message in the database.
                    # Clean up the entry, and proceed as usual.

                    _logger.info("Found process is stale, recovering "
                                 "id=%s path=%s filename=%s",
                                 id_, path, filename)

                    _recover_pending_entry(
                        id_, os.path.join(path, filename), cursor)
                else:
                    # The other process really still exists, so don't run
                    # an update ourselves.
                    _logger.info(
                        "Other process still exists, will not run update.")
                    run_update = False

            if run_update:
                # So we have decided to run a metadata update (because no other
                # update is happening in parallel right now). Add a pending
                # metadata row in the database before releasing the lock, so
                # that other processes will wait for us instead of starting
                # their own metadata update.

                # First, get the previous metadata, if any, and find out which
                # version it is.

                _logger.info("Preparing to run update. "
                             "Getting previous metadata to determine version.")

                prev_version = None
                prev_metadata_build_version = None
                for index in itertools.count():
                    try:
                        cursor.execute('SELECT filename FROM Metadata '
                                       'WHERE pending IS NULL '
                                       'ORDER BY update_time DESC LIMIT ?, 1',
                                       (index,))
                        prev_metadata = cursor.fetchone()
                    except Exception:
                        break

                    if not prev_metadata:
                        break

                    filename, = prev_metadata
                    try:
                        prev_db = _open_metadata_db(
                            os.path.join(path, filename), 'r')
                        try:
                            prev_version, prev_metadata_build_version = \
                                _get_metadata_version_info(prev_db)
                            break
                        except Exception as e:
                            _logger.exception("Failed to get version info")
                        finally:
                            prev_db.close()
                    except Exception:
                        # This throws if the SQLite schema version is not
                        # right, or we could not open the previous database.
                        # TODO: attempt to cleanup the stale entry?
                        pass

                # Choose a random name for the new metadata database:
                while True:
                    random_name = ''.join(
                        [chr(97 + random.choice(range(26))) for _ in range(16)])
                    new_filename = f'{random_name}.db'
                    if not os.path.exists(os.path.join(path, new_filename)):
                        break

                pid = mserver.open()

                _logger.info("Creating pending entry: filename=%s "
                             "update_time=%s pid=%s", new_filename, now, pid)

                # Insert pending entry to index.db
                cursor.execute("INSERT INTO Metadata "
                               "(filename, update_time, writer_pid, pending) "
                               "VALUES (?, ?, ?, 1)", (new_filename, now, pid))
                new_id = cursor.lastrowid
        finally:
            # release lock
            cursor.execute('COMMIT')

        if not run_update:
            # after releasing the database lock, wait for the other update
            # process to finish. We'll just re-use its result instead of
            # running the update on our own.
            _logger.info("Will not run update... "
                         "Waiting for other process to finish update...")
            mclient.wait()

    # After we released the database lock, start the actual
    # metadata update procedure if there was no other process running
    # already.
    if run_update:
        try:
            try:
                _logger.info(
                    "Running update, fetching new metadata from service...")
                metadata = deserializer.load_metadata(
                    prev_version=prev_version,
                    prev_metadata_build_version=prev_metadata_build_version)
            except MetadataDeserializer.NotModified:
                _logger.info("Metadata is up to date. Will not download.")
                have_new_database = False
            else:
                _logger.info("Done loading metadata, opening db path=%s "
                             "filename=%s", path, new_filename)
                md_db = _open_metadata_db(
                    os.path.join(path, new_filename), 'rw')
                try:
                    _logger.info("Storing metadata to db")
                    with ScopedTracer(name='bqlmetadata.dbstore',
                                      logger=_logger,
                                      publish=True):
                        serializer = SqliteMetadataSerializer(md_db)
                        serializer.store_metadata(metadata)
                except Exception as ex:
                    # The update failed. Attempt to delete the partly written
                    # metadata database file.
                    _logger.warning("Failed to store metadata. Recovering...")
                    try:
                        _unlink(os.path.join(path, new_filename))
                    except Exception as unlink_ex:
                        new_path = os.path.join(path, new_filename)
                        _logger.warning(
                            'Failed to delete partly written database "%s": %s',
                            new_path,
                            str(unlink_ex),
                            exc_info=sys.exc_info())
                    raise
                else:
                    _logger.info("Done storing metadata.")
                    have_new_database = True
                finally:
                    md_db.close()

        except Exception as ex:
            # The update failed. Set an error message on
            # the pending entry in the index.db.
            _logger.exception("Failed to update metadata.")
            cursor.execute(
                'UPDATE Metadata SET error_message = ? WHERE id = ?',
                (str(ex), new_id))
            raise

        if have_new_database:
            # we actually have a new database. Remove the pending flag
            # from the index entry so that the new database becomes current.
            _logger.info("Removing pending flag...")
            cursor.execute('UPDATE Metadata SET pending = NULL WHERE id = ?',
                           (new_id,))

        else:
            # We don't have a new database. This means there's just no new
            # metadata on the server and we are good with what we have.
            # Just update the timestamp of the previous metadata entry (so we
            # try to refresh next time only in 24h) and delete the pending
            # new database.
            cursor.execute('BEGIN IMMEDIATE')
            try:
                cursor.execute('DELETE FROM Metadata WHERE id = ?', (new_id,))
                if id_ is not None:
                    cursor.execute(
                        "UPDATE Metadata SET update_time = ? WHERE id = ?",
                        (now, id_))
            finally:
                cursor.execute('COMMIT')

    # At the end, even if we didn't end up running an update because either
    # we just waited for another process or because we determined that there
    # was no new metadata, attempt to clean up all outdated metadata entries.
    # We might not have been able to do this earlier, such as after the update
    # which caused an entry to become outdated, due to that entry still being
    # used by another process.
    _cleanup_db(index_db, path)


def _update_metadata_cache(deserializer, path, mclient, mserver):
    """Same as above but instead it takes a filesystem path"""

    try:
        _makedirs(path)
        _logger.info("Opening index.db in path %s", path)
        db = _open_index_db(os.path.join(path, 'index.db'), 'rw')
        try:
            with mserver:
                _update_metadata_cache_with_index_db(deserializer,
                                                     path,
                                                     db,
                                                     mclient,
                                                     mserver)
        finally:
            db.close()

        _logger.info("Successfully updated metadata cache.")
        return 0
    except Exception as ex:
        # We are logging the error here in addition to having the exception
        # message written into the database.
        _logger.error("Exception while updating metadata: %s",
                      ex,
                      exc_info=sys.exc_info())
        return 1


def _update_metadata_cache_default():
    """
    Same as _update_metadata_cache, but use default values for parameters.
    """
    from .process_monitor import (TcpProcessMonitorClient,
                                  TcpProcessMonitorServer)

    from ..default_metadata_serializer import DefaultMetadataDeserializer

    _logger.info("Starting metadata cache update. Creating default "
                 "metadata deserializer with args: %s", sys.argv[1:])

    bqapi_session = None
    deserializer = DefaultMetadataDeserializer(bqapi_session, *sys.argv[1:])

    path = os.environ.get('BQL_METADATA_CACHE_DIR')
    mserver = TcpProcessMonitorServer()
    mclient = TcpProcessMonitorClient()

    if not path:
        _logger.error("BQL_METADATA_CACHE_DIR is not set")
        raise RuntimeError('BQL_METADATA_CACHE_DIR not set')

    return _update_metadata_cache(deserializer, path, mclient, mserver)
