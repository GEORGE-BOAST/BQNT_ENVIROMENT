#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import contextlib
import os
import logging
import sqlite3

from ..metadata_reader import MetadataReader
from ..memory_metadata_reader import MemoryMetadataReader
from .database import (UnsupportedVersionError,
                       _open_index_db,
                       _open_metadata_db)

from .process_monitor import TcpProcessMonitorServer, TcpProcessMonitorClient
from .bulk_loader import BulkMetadataLoader

from ..metadata_update_functions import default_metadata_update

from bqutil import ScopedTracer, trace_it


logger = logging.getLogger(__name__)


class SqliteCacheMetadataReader(MetadataReader):
    """Load metadata and cache it locally.

    This class is a :class:`MetadataReader` which loads the metadata from
    a deserializer (e.g. from JSON format received from the BQL metadata
    service) and stores it in a SQLite database at a known location.
    If the database exists already, it does not load anything but simply
    serves from the database.

    The cache resides in a directory with a index.db file and one or more
    actual metadata databases. Only one of the metadata databases is active
    at a time, and the index.db stores which one this is. While the cache is
    being updated, there will be another metadata database file under
    construction. Once constructed, the index.db file is updated to point to
    the new database file, and the old is deleted at the earliest convenience.

    The nice thing about this approach is that we can rely on SQLite to do
    much of the file locking and dealing with concurrency situations, and
    prevent corruption of the data in case the process gets terminated while
    updating the metadata.
    """

    def __init__(self, deserializer, path, update_func=None):
        """Create an instance of the SqliteCacheMetadataReader.

        The given path and the index.db file will be created if they do
        not exist.

        The `update_func` argument optionally specifies a function that will
        be called with 4 arguments when a metadata update is being initiated.
        The function should return an object with a `wait()` method that can
        be used to wait for the completion of the update. The 4 arguments
        passed to the function are the deserializer and path passed to the
        constructor, as well as a monitor client and monitor server.
        """
        self.__update_func = update_func or default_metadata_update
        self.__metadata_db_filename = None
        self.__metadata_db = None
        self.__reader = None
        self.__path = path
        with ScopedTracer(name="bqlmetadata.update",
                          logger=logger,
                          publish=True):
            try:
                # Always do the update in a background process, so that even if
                # our process terminates during the update, the metadata update
                # can finish in the background.  Use subprocess instead of the
                # multiprocessing module so the child process actually outlives
                # us.
                process = self.__update_func(deserializer, self.__path,
                                             TcpProcessMonitorClient(),
                                             TcpProcessMonitorServer())
                process.wait()
            except Exception as e:
                # If the background process fails, then we still have the 
                # opportunity to fall back on previously cached metadata in
                # the call to __open_metadata_db() below; so we don't want
                # to treat exceptions as fatal.  We'll log the error through
                # our backend services so that we are aware of the error, but
                # prevent the error message from being printed in the notebook.
                logger.error(e, extra={'suppress': True})
                pass
            try:
                self.__open_metadata_db()
            except Exception:
                # If an exception occurs during the update, make sure the
                # database is closed. After initialization, the caller is
                # responsible for calling our close() method if they want to
                # make sure the database is closed.
                self.close()
                raise

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        """Close the matadata cache.

        Call this to make sure all file handles on the databasases are
        released.
        """
        self.__reader = None
        if self.__metadata_db is not None:
            self.__metadata_db.close()
            self.__metadata_db = None
        self.__metadata_db_filename = None

    @property
    def _filename(self):
        return self.__metadata_db_filename

    def get_bulk_metadata(self):
        return self.__reader.get_bulk_metadata()

    def get_version(self):
        return self.__reader.get_version()

    def _get_by_name(self, what, name):
        metadata_items = self.__reader._get_by_name(what, name)
        # Make the MetadataItem objects look as if they were read by the
        # cached reader.
        return [self.__override_metadata_reader(m) for m in metadata_items]

    def _get_by_operator(self, symbol, location):
        metadata_item = self.__reader._get_by_operator(symbol, location)
        # Make the MetadataItem look as if it were read by the cached reader.
        return self.__override_metadata_reader(metadata_item)

    def _enumerate_names(self, what):
        return self.__reader._enumerate_names(what)

    def __override_metadata_reader(self, metadata_item):
        # Make the MetadataItem look as if it were read by the cached reader.
        # We're doing an in-place modification because we know that
        # metadata_item came from self.__reader, and we're the only ones that
        # access self.__reader.
        metadata_item._set_metadata_reader(self)
        return metadata_item

    @trace_it(logger=logger, publish=True)
    def __open_metadata_db(self):
        # If there was no index.db to begin with, it should have been created
        # now. If not, the metadata update process must have failed at a very
        # early stage.
        index_path = os.path.join(self.__path, 'index.db')
        with contextlib.closing(_open_index_db(index_path, "r")) as index_db:
            cursor = index_db.cursor()
            cursor.execute('BEGIN')
            try:
                cursor.execute("""
                    SELECT filename, update_time, pending, error_message
                    FROM Metadata
                    ORDER BY update_time DESC
                """)
                rows = cursor.fetchall()
                for (filename, update_time, pending, error_message) in rows:
                    # We never expect __open_metadata_db() to be called when
                    # we're already using a database.  And we expect that if
                    # we find a database that we can use, then we stop
                    # iterating through this loop.
                    assert self.__metadata_db is None
                    assert self.__metadata_db_filename is None
                    assert self.__reader is None
                    if pending is None:
                        self.__create_reader(filename)
                        if self.__reader is not None:
                            # We've successfully created a metadata reader, so
                            # we're done.
                            return
                        else:
                            # This database didn't work, but try the next one.
                            pass
                    elif error_message is not None:
                        # There was an error while this metadata was being
                        # updated.  Try the next database after recording the
                        # error.
                        logger.warning(
                            "%s: Error while updating metadata at %s: %s",
                            filename, update_time, error_message,
                            extra={'suppress': True})
                    else:
                        # Someone else is in the process of creating a new
                        # metadata database, so it's not usable yet.  Move
                        # on to the next database.
                        pass
                # We've examined all the databases in index.db and none of
                # them are suitable for using.
                raise RuntimeError("No metadata available")
            finally:
                cursor.execute('COMMIT')

    def __create_reader(self, filename):
        # Initializes self.__metadata_db, self.__metadata_filename,
        # and self.__reader if successful.
        assert self.__metadata_db is None
        assert self.__metadata_db_filename is None
        assert self.__reader is None
        db_path = os.path.join(self.__path, filename)
        try:
            self.__metadata_db = _open_metadata_db(db_path, 'r')
            loader = BulkMetadataLoader(self.__metadata_db)
        except (UnsupportedVersionError, IOError) as e:
            # Don't let this exception keep us from trying
            # the next database, but log the error.
            logger.warning("%s: %s", filename, e)
        else:
            self.__metadata_db_filename = filename
            self.__reader = MemoryMetadataReader(loader.load_metadata())

    def _search_items(self, query, what, max_items):
        """
        `query` argument is the substring to search for within the name/mnemonic and/or description of items
        `what` argument is a tuple of ``'function'``, ``'data-item'``, ``'universe-handler'``,
        or a combination of them ``'all'``.
        `max_items` is the number of max items per category.
        """
        query = query.lower()
        queryLike = "%" + query + "%"
        category_props = {
            'function': {'table': 'Functions', 'name_expr': 'upper(name)', 'condition': 'id NOT IN (SELECT func FROM Operators)'},
            'data-item': {'table': 'DataItems', 'name_expr': 'upper(IFNULL(mnemonic, name))', 'condition': None},
            'universe-handler': {'table': 'UniverseHandlers', 'name_expr': 'upper(name)', 'condition': 'lower(name) <> \'for\''}
        }
        category_limit = max_items
        unranked_limit = 1000
        results = []

        def list_row_factory(cursor, row):
            return [row[idx] for idx, col in enumerate(cursor.description)]

        def build_search_query(category_name, table, name_expr, condition):
            return """
                SELECT * FROM (SELECT name, description, '{category_name}' AS category, 5.0 * (1.0 / CAST(rank1 AS float) + 1.0 / CAST(rank2 AS float)) AS rank FROM (
                    SELECT name, description, CASE WHEN name_rank=0 THEN 10000 ELSE name_rank END AS rank1, CASE WHEN desc_rank=0 THEN 10000 ELSE desc_rank END AS rank2 FROM (
                        SELECT {name_expr} AS name, description, INSTR(lower({name_expr}), ?) name_rank, INSTR(lower(description), ?) desc_rank FROM {table}
                        WHERE {condition_expr} ({name_expr} LIKE ? OR description LIKE ?) LIMIT {unranked_limit}
                    )
                ) ORDER BY rank DESC LIMIT ?)""".format(
                    table=table, name_expr=name_expr, category_name=category_name,
                    condition_expr=((condition + ' AND ') if condition != None else ''),
                    unranked_limit=unranked_limit
                )

        conn = self.__metadata_db
        prev_row_factory = conn.row_factory
        conn.row_factory = list_row_factory
        cur = None

        try:
            cur = conn.cursor()
            sql = " UNION ALL ".join(
                build_search_query(cat,
                                   category_props[cat]['table'],
                                   category_props[cat]['name_expr'],
                                   category_props[cat]['condition']
                ) for cat in what
            )
            args = tuple(arg for categories in what for arg in (query, query, queryLike, queryLike, category_limit))

            if sql:
                cur.execute(sql, args)
                results = cur.fetchall()
        except sqlite3.Error as e:
            logger.error("Database error: %s" % e)
            raise
        except Exception as e:
            logger.error("Exception occurred: %s" % e)
            raise
        finally:
            if cur:
                cur.close()
            conn.row_factory = prev_row_factory

        return results
