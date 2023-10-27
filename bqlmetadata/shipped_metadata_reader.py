# Copyright 2020 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .memory_metadata_reader import MemoryMetadataReader
from .shipped_metadata_serializer import ShippedMetadataDeserializer


class ShippedMetadataReader(MemoryMetadataReader):
    def __init__(self):
        deserializer = ShippedMetadataDeserializer()
        metadata = deserializer.load_metadata()
        super(ShippedMetadataReader, self).__init__(metadata)
