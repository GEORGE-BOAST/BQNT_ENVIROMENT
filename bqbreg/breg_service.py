from abc import ABCMeta, abstractmethod

from bqrequest import (BQGWSVC_MAJOR_VERSION,
                       BQGWSVC_MINOR_VERSION)

import logging
import six

_logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class BregService():

    @abstractmethod
    def evaluate_breg(self, accessor_name, entry_id, breg_type):
        raise NotImplementedError()

    @staticmethod
    def _make_request_payload(accessor_name, entry_id, breg_type):
        return {
            'clientInfo': {
                'version': {
                    'major': BQGWSVC_MAJOR_VERSION,
                    'minor': BQGWSVC_MINOR_VERSION
                }
            },
            'requestData': {
                'checkBregRequest': {
                    'accessorName': accessor_name,
                    'entryId': entry_id,
                    'bitType': breg_type
                }
            }
        }

    @staticmethod
    def _get_value_from_response(resp):
        """
        resp is a GwResponse structure from bqgwsvc
        """

        if 'responseData' not in resp and 'status' not in resp:
            err = "Invalid response from bqgwsvc: {}".format(resp)
            _logger.error(err)
            raise RuntimeError(err)

        if resp['status']['code'] != "SUCCESS":
            err = "Error from bqgwsvc: {}".format(resp['status'])
            _logger.error(err)
            raise RuntimeError(err)

        respData = resp['responseData']

        if 'checkBregResponse' not in respData:
            err = "Unexpected response from bqgwsvc: {}".format(respData)
            _logger.error(err)
            raise RuntimeError(err)

        bitValue = next(iter(
            respData['checkBregResponse']['bitValue'].items()))

        valueType, value = bitValue
        return (value['values']
                if valueType in ('intList', 'extendedIntList')
                else value)
