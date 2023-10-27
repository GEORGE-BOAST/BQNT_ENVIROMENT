# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .metadata_serializer import MetadataDeserializer
from .json_metadata_loader import JsonMetadataLoader
from .version_util import VersionInfo


class FileMetadataDeserializer(MetadataDeserializer):
    """Load metadata from the given JSON files."""
    def __init__(self, file_names, version, schema_version):
        self.__file_names = file_names
        self.__version = version
        self.__schema_version = schema_version

    def _load_metadata(self, prev_version):
        loader = JsonMetadataLoader(self.__version, self.__schema_version)
        for file_name in self.__file_names:
            with open(file_name, "rt") as f:
                loader.load(f)
        return loader.metadata

    def _get_version(self):
        return VersionInfo(self.__version, self.__schema_version, None)
