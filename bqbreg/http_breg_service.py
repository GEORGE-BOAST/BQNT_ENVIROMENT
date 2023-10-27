from .breg_service import BregService
from bqrequest import (HttpServiceClient,
                       ServiceInfo,
                       BQGWSVC_MAJOR_VERSION,
                       BQGWSVC_MINOR_VERSION)

import logging

_logger = logging.getLogger(__name__)


class HttpBregService(BregService):

    def __init__(self, host):
        self._service = HttpServiceClient(
            ServiceInfo('bqgwsvc',
                        BQGWSVC_MAJOR_VERSION,
                        BQGWSVC_MINOR_VERSION), host)

    def evaluate_breg(self, accessor_name, entry_id, breg_type):
        payload = BregService._make_request_payload(
            accessor_name, entry_id, breg_type)
        request = {'gwRequest': payload}
        resps = self._service.send_request(request)

        if len(resps) != 1:
            _logger.error("Unexpected response from breg servce: %s", resps)
            raise RuntimeError("Unexpected response from breg service")

        resp = resps[0]

        if 'gwResponse' not in resp:
            err = "Invalid response from bqgwsvc: {}".format(resp)
            _logger.error(err)
            raise RuntimeError(err)

        return BregService._get_value_from_response(resp['gwResponse'])
