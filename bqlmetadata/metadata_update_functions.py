# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os
import subprocess
import sys
import six
import base64
import pickle
import logging

from . import DefaultMetadataDeserializer
from .sqlite.process_monitor import (TcpProcessMonitorClient,
                                     TcpProcessMonitorServer)

from .sqlite.cache_update import _update_metadata_cache


_UPDATE_TASK_VERBOSITY = 3

_logger = logging.getLogger(__name__)


class _NoOpProcess(object):
    def wait(self):
        pass


def no_op_metadata_update(deserializer, path, monclient, monserver):
    return _NoOpProcess()


def default_metadata_update(deserializer, path, mclient, mserver):
    pickle_args = pickle.dumps((_update_metadata_cache,
                                (deserializer, path, mclient, mserver)))
    if six.PY2:
        args = [sys.executable, '-c',
                'import sys, pickle; '
                'func, args = pickle.loads(sys.argv[-1]); '
                'func(*args)',
                pickle_args]
    else:
        args = [sys.executable, '-c',
                'import sys, pickle, base64; '
                'func, args = pickle.loads(base64.b64decode(sys.argv[-1].encode("ASCII"))); '
                'func(*args)',
                base64.b64encode(pickle_args).decode('ASCII')]

    return subprocess.Popen(args)


def trusted_metadata_update(deserializer, path, mclient, mserver):
    """A function to run a metadata update from within the BQNT sandbox.

    The standard update procedure is to spawn a new python process that
    performs the actual metadata update. This does not work from within the
    BQNT sandbox since process creation is not allowed, and also the metadata
    directory is not writable by the python process.

    Instead, the sandbox provides an interface by exposing a trusted binary
    that can be executed to perform a metadata cache update. This update
    function can be passed to SqliteMetadataCacheReader in order to use this
    functionality.

    This update function is used as the default if the update process is
    running withint the BQNT sandbox.
    """
    # We cannot pass any python types for deserializer and process monitors
    # to the process running outside of the sandbox. Therefore, just make
    # sure that we were called with the default ones.
    if not isinstance(deserializer, DefaultMetadataDeserializer):
        raise TypeError('Deserializer is not a `DefaultMetadataDeserializer`')

    if not isinstance(mclient, TcpProcessMonitorClient):
        raise TypeError('Monitor client is not of type `TcpProcessMonitorClient`')

    if not isinstance(mserver, TcpProcessMonitorServer):
        raise TypeError('Monitor server is not of type `TcpProcessMonitorServer`')

    cache_path = os.environ.get('BQL_METADATA_CACHE_DIR', None)
    if not cache_path or not (path == cache_path or
                              path.startswith(cache_path + os.sep)):
        raise ValueError('Path not in approved BQL_METADATA_CACHE_DIR')

    # The kernel executable may not always be located in the same directory as the
    # sandbox_bql_metadata_cache_update.exe. For that reason we need to first check
    # if the executable is in the bqplatform root directory before we default to
    # looking at the executable location.
    exe = 'sandbox_bql_metadata_cache_update.exe'
    exe_path = os.environ.get('BIPY_BQPLATFORM_DIR', None)
    if (not exe_path or
        not sys.executable.startswith(exe_path) or
        not os.path.exists(os.path.join(exe_path, exe))):
        exe_path = os.path.dirname(sys.executable)
    executable = os.path.join(exe_path, exe)
    if not os.path.exists(executable):
        raise FileNotFoundError(f'{exe} not found in standard folders')

    args = [executable, "-v", str(_UPDATE_TASK_VERBOSITY)]

    _logger.info("Starting update task: %s", args)

    return subprocess.Popen(args + list(deserializer.args))
