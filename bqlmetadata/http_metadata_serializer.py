#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import codecs
import json

from contextlib import closing

from six.moves.urllib.request import urlopen, Request
from six.moves.urllib.parse import urlencode

from .metadata_serializer import MetadataDeserializer, VersionInfo
from .json_metadata_loader import JsonMetadataLoader
from .version_util import parse_version, parse_schema_version


def make_bql_http_request(url, operation, params, timeout=None, method='POST'):
    """Make an HTTP request to a BQL endpoint."""

    if method.lower() == 'post':
        final_url = f'{url}/{operation}'
        data = json.dumps(params).encode('UTF-8')
    else:
        final_url = f'{url}/{operation}?{urlencode(params)}'
        data = None

    req = Request(final_url, data=data, headers={'Content-Type': 'application/json'})

    # application/json should give us back a utf-8 response.
    reader = codecs.getreader("utf-8")

    return reader(urlopen(req, timeout=timeout))


class HttpMetadataDeserializer(MetadataDeserializer):
    _METADATA_RESOURCE = "/BQLMetaDataService/api/v2"

    """Load metadata via an HTTP REST API."""
    def __init__(self, url, timeout=None):
        self._url = f"{url}{HttpMetadataDeserializer._METADATA_RESOURCE}"
        self._timeout = timeout

    @staticmethod
    def open_url(url, timeout=None):
        deserializer = HttpMetadataDeserializer(url, timeout)
        return deserializer.__open_url()

    def _load_metadata(self, prev_version):
        version, schema_version = self._get_version()
        with self.__open_url() as f:
            loader = JsonMetadataLoader(version, schema_version)
            loader.load(f)
            return loader.metadata

    def _get_version(self):
        with make_bql_http_request(self._url, 'getVersion',
                                   {}, self._timeout, method='GET') as f:
            result = json.load(f)
            response = result['metadataVersionResponse']

            return VersionInfo(
                parse_version(response['version']),
                parse_schema_version(response['responseSchemaVersion']),
                None)

    def __open_url(self):
        return make_bql_http_request(
            self._url, 'getAllMetadataOfType', {'option': 'ALL'},
            self._timeout, method='GET')
