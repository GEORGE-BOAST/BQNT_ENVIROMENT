#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os

from .bqapi_metadata_serializer import BqapiMetadataDeserializer
from .http_metadata_serializer import HttpMetadataDeserializer
from .metadata_serializer import MetadataDeserializer
from .shipped_metadata_serializer import ShippedMetadataDeserializer


class DefaultMetadataDeserializer(MetadataDeserializer):
    def __init__(self, bqapi_session=None, *args):
        self.args = args
        if args and args[0] == 'initial':
            self._underlying = ShippedMetadataDeserializer()
        else:
            try:
                metadata_url = os.environ['PYBQL_DEFAULT_METADATA_URL']
            except KeyError:
                self._underlying = BqapiMetadataDeserializer(bqapi_session)
            else:
                self._underlying = HttpMetadataDeserializer(metadata_url)

    def _load_metadata(self, prev_version):
        return self._underlying._load_metadata(prev_version)

    def _get_version(self):
        return self._underlying._get_version()
