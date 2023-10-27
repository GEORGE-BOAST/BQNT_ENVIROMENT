# Copyright 2018 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os
import platform

import bqbreg

from . import HttpMetadataDeserializer, DefaultMetadataDeserializer
from .sqlite import SqliteCacheMetadataReader

from .metadata_update_functions import (
    default_metadata_update,
    trusted_metadata_update
)

from .bqhopper_bqapi_metadata_deserializer import (
    BqhopperBqapiMetadataDeserializer
)
from .bqhopper_partial_metadata_reader import BqhopperPartialMetadataReader
from .bqhopper_metadata_reader import BqhopperMetadataReader

from .shipped_metadata_reader import ShippedMetadataReader


_ENABLE_BQHOPPER = None


def _get_bqhopper_enablement():
    global _ENABLE_BQHOPPER
    if _ENABLE_BQHOPPER is None:
        _ENABLE_BQHOPPER = bqbreg.bqbreg_eval_int(
            'bbit_enable_bqhopper',
            414409,
            default=1)
    return _ENABLE_BQHOPPER


class MetadataReaderFactory:
    """Factory class that determines the metadata reader object used to
    fetch and process metadata from BQL.
    """

    def __init__(self):
        self._update_func = default_metadata_update

        # If we are running in the bipy environment, i.e. in the BQNT
        # sandbox, then swap out the metadata update function with
        # something that invokes the trusted metadata updater.

        # TODO: better way to detect running in BQNT sandbox
        if (platform.system() == 'Windows' and
            'BIPYDIR' in os.environ and
            # trusted_metadata_update relies on BQL_METADATA_CACHE_DIR.
                'BQL_METADATA_CACHE_DIR' in os.environ):
            self._update_func = trusted_metadata_update

        self._cache_location = os.environ.get('BQL_METADATA_CACHE_DIR')
        if not self._cache_location:
            self._cache_location = os.path.join(os.path.expanduser('~'),
                                                '.bqlmetadata-cache')

    def create_metadata_reader(self):
        import bqapi

        bqapi_session = bqapi.get_session_singleton()

        bqhopper_breg_value = _get_bqhopper_enablement()

        if bqhopper_breg_value == 0:
            # BREG value of 0 will use the legacy, local SQLite
            # metadata solution or shipped metadata.
            if os.environ.get('BQNT_BQL_SHIPPED_METADATA') == '1':
                reader = ShippedMetadataReader()

            else:
                deserializer = DefaultMetadataDeserializer(
                    bqapi_session=bqapi_session)
                reader = SqliteCacheMetadataReader(deserializer,
                                                   self._cache_location,
                                                   self._update_func)

        elif bqhopper_breg_value == 2:
            # Use the partial-metadata (long-term) bqhopper solution.
            deserializer = BqhopperBqapiMetadataDeserializer(
                bqapi_session=bqapi_session)
            reader = BqhopperPartialMetadataReader(deserializer)

        else:
            # Use the full-metadata (intermediate) bqhopper solution.
            deserializer = BqhopperBqapiMetadataDeserializer(
                bqapi_session=bqapi_session)
            reader = BqhopperMetadataReader(deserializer)

        return reader

    def create_http_metadata_reader(self, url):
        if not url:
            raise ValueError("URL required to create http metadata reader")

        return SqliteCacheMetadataReader(HttpMetadataDeserializer(url),
                                         self._cache_location,
                                         self._update_func)
