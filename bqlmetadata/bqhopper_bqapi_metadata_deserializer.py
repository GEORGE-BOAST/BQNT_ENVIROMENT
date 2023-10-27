# Copyright 2020 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import base64
import json
import logging
import zlib

from bqrequest import BlpapiServiceClient

from .exceptions import ServiceError, StaleMetadataError


_logger = logging.getLogger(__name__)


_METADATA_SVC = '//blp/bqhopper'


def _decompress_metadata(metadata):
    metadata_compressed = metadata.encode(encoding='latin-1')
    metadata_compressed = base64.b64decode(metadata_compressed)
    metadata_decompressed = zlib.decompress(metadata_compressed)
    metadata_json = metadata_decompressed.decode(encoding='utf-8')

    return json.loads(metadata_json)


class BqhopperBqapiMetadataDeserializer:
    def __init__(self, bqapi_session=None):
        self._svc = BlpapiServiceClient(_METADATA_SVC,
                                        bqapi_session=bqapi_session)
        _logger.info('Metadata service is: %s', _METADATA_SVC)

    def load_partial_metadata(self, namespace=None):
        request_type = 'getPartialMetadataRequest'
        payload = {'clientId': 'BQNT'}

        if namespace:
            payload.update({'namespace': namespace})

        # Make request to bqhopper
        response, *_ = self._send_request(request_type, payload)

        # Parse response
        entities = response.get('entities', [])
        metadata_deserialized = _decompress_metadata(response['metadata'])
        partial_metadata_id = response.get('partialMetadataId')

        return {
            'entities': entities,
            'metadata': metadata_deserialized,
            'partial_metadata_id': partial_metadata_id
        }

    def load_metadata(self):
        request_type = 'getAllMetadataRequest'
        payload = {
            'clientId': 'BQNT',
            'cachedMetadataVersion': 2
        }

        # Make request to bqhopper
        response, *_ = self._send_request(request_type, payload)

        # Parse response
        metadata_serialized = response['metadata']
        metadata_deserialized = _decompress_metadata(metadata_serialized)

        return {
            'metadata': metadata_deserialized
        }

    def get_metadata_for_entity(self,
                                entity_key,
                                partial_metadata_id,
                                entity_type,
                                namespace=None):
        request_type = 'getMetadataForEntityRequest'

        payload = {
            'entityKey': entity_key,
            'partialMetadataId': partial_metadata_id,
            'clientId': 'BQNT',
            'entityType': entity_type,
        }

        if namespace:
            payload.update({'namespace': namespace})

        response, *_ = self._send_request(request_type, payload)
        entity_md = _decompress_metadata(response['metadata'])

        return entity_md

    def _send_request(self, request_type, payload):
        responses = self._svc.send_request(request_type, payload)

        # Error responses will have a returnStatus.status of "FAILURE".
        return_status = responses[0]['returnStatus']

        if return_status['status'] == 'FAILURE':
            error_msg = return_status['errorMessage']

            if return_status.get('errorCode') == 'STALE_METADATA':
                _logger.info('StaleMetadataError: %s', error_msg)
                raise StaleMetadataError(error_msg)
            else:
                _logger.error('ServiceError: %s', error_msg)
                raise ServiceError(error_msg)

        return responses
