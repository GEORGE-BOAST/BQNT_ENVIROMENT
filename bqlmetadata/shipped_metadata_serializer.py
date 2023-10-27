#!/usr/bin/env python

# Copyright 2017 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os

from .metadata_serializer import MetadataDeserializer
from .minjson_metadata_serializer import MinJsonMetadataDeserializer


_this_path = os.path.dirname(os.path.abspath(__file__))


class ShippedMetadataDeserializer(MetadataDeserializer):
    def __init__(self):
        path = os.path.join(_this_path, 'metadata.json.bz2')
        self._underlying = MinJsonMetadataDeserializer(path)

    def _load_metadata(self, prev_version):
        return self._underlying._load_metadata(prev_version=prev_version)

    def _get_version(self):
        return self._underlying._get_version()
