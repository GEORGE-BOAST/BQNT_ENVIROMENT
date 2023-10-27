#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .metadata_serializer import MetadataDeserializer


class CachedMetadataDeserializer(MetadataDeserializer):
    """Cache result of another metadata deserializer.
    
    This is so that it can be used multiple times, e.g. for multiple unit
    tests.
    """
    def __init__(self, deserializer):
        self._deserializer = deserializer
        self._metadata = None

    def _load_metadata(self, prev_version):
        if self._metadata is None:
            self._metadata = self._deserializer.load_metadata(prev_version)
        return self._metadata

    def _get_version(self):
        return self._deserializer._get_version()
