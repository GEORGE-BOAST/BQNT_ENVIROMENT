# Copyright 2017 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .memory_metadata_reader import MemoryMetadataReader
from .http_metadata_serializer import HttpMetadataDeserializer


class HttpMetadataReader(MemoryMetadataReader):
    def __init__(self, url, timeout=None):
        deserializer = HttpMetadataDeserializer(url, timeout)
        metadata = deserializer.load_metadata()
        super(HttpMetadataReader, self).__init__(metadata)
