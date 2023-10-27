# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os
import subprocess
import sys

from bqlmetadata import DefaultMetadataDeserializer
from bqlmetadata.sqlite.process_monitor import (TcpProcessMonitorClient,
                                                TcpProcessMonitorServer)


def _trusted_update(deserializer, path, mclient, mserver):
    # A function to run a metadata update from within the BQNT sandbox.
    
    # The standard update procedure is to spawn a new python process that
    # performs the actual metadata update. This does not work from within the
    # BQNT sandbox since process creation is not allowed, and also the metadata
    # directory is not writable by the python process.
     
    # Instead, the sandbox provides an interface by exposing a trusted binary
    # that can be executed to perform a metadata cache update. This update
    # function can be passed to SqliteMetadataCacheReader in order to use this
    # functionality.
     
    # Note that for default-constructed :class:`Service` instances, this update
    # function is installed by default on the metadata reader if the process is
    # running within the BQNT sandbox.

    # We cannot pass any python types for deserializer and process monitors
    # to the process running outside of the sandbox. Therefore, just make
    # sure that we were called with the default ones.
    if not isinstance(deserializer, DefaultMetadataDeserializer):
        raise TypeError('Deserializer is not a `DefaultMetadataDeserializer`')

    if not isinstance(mclient, TcpProcessMonitorClient):
        raise TypeError('Monitor client is not of type `TcpProcessMonitorClient`')

    if not isinstance(mserver, TcpProcessMonitorServer):
        raise TypeError('Monitor server is not of type `TcpProcessMonitorServer`')

    cache_path = os.environ.get('BQL_METADATA_CACHE_DIR')
    if not cache_path or (path != cache_path and not path.startswith(cache_path + os.sep)):
        raise ValueError('Path not in approved BQL_METADATA_CACHE_DIR')

    executable = os.path.join(os.path.dirname(sys.executable),
                              'sandbox_bql_metadata_cache_update.exe')

    return subprocess.Popen(args=[executable] + list(deserializer.args))
