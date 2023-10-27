# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .exceptions import ServiceError
from .metadata_serializer import MetadataDeserializer, _make_metadata_request
from .json_metadata_loader import JsonMetadataLoader
from .version_util import build_version_info_from_response
from bqutil import ScopedTracer, trace_it

import logging

_logger = logging.getLogger(__name__)

_METADATA_SVC = '//blp/bqldsvc'


class BqapiMetadataDeserializer(MetadataDeserializer):
    """Load metadata from the //blp/bqldsvc BLPAPI API service."""

    def __init__(self, bqapi_session=None):
        if bqapi_session is None:
            import bqapi
            bqapi_session = bqapi.get_session_singleton()
        self._session = bqapi_session
        _logger.info("Metadata service is: %s", _METADATA_SVC)

    def _load_metadata(self, prev_version):
        version_info = self._get_version()
        with ScopedTracer(name='bqlmetadata.download',
                          logger=_logger,
                          publish=True):
            metadata = self.get_metadata_json()
        with ScopedTracer(name='bqlmetadata.serialization',
                          logger=_logger,
                          publish=True):
            loader = JsonMetadataLoader(version_info.version,
                                        version_info.schema_version,
                                        version_info.metadata_build_version)
            loader.load_chunks(metadata)
        return loader.metadata

    @trace_it(logger=_logger, name="bqlmetadata.get_version", publish=True)
    def _get_version(self):
        version_info = self.__send_request(
            'getVersion', {'version': 'version'})
        return build_version_info_from_response(version_info[0])

    # Make this object pickleable so it can be used for out-of-process
    # metadata updates. Simply pickle the settings of the bqapi session,
    # and then re-instantiate a new session with the same settings.
    def __getstate__(self):
        return (self._session.settings, )

    def __setstate__(self, state):
        import bqapi

        settings, = state
        self._session = bqapi.Session(settings=settings)

    def get_metadata_json(self):
        return self.__send_request(
            'getCoreMetadataOfType',
            _make_metadata_request(),
        )

    def __send_request(self, request_type, payload):
        responses = self._session.send_request(_METADATA_SVC,
                                               request_type,
                                               payload)
        # All responses should be of the same type.
        message_type = responses[0].message_type()
        for resp in responses[1:]:
            if resp.message_type() != message_type:
                raise ServiceError(
                    f'Partial responses are of mixed types {message_type} '
                    f'and {resp.message_type()}')
        # Look for an errorResponse from bqldsvc: these are simple strings
        # that are represented as bqapi Message objects not as dict-like
        # MessageElement objects.
        if message_type == 'errorResponse':
            error_msg = ''.join([resp.element() for resp in responses])
            raise ServiceError(error_msg)
        return responses
