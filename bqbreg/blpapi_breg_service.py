from .breg_service import BregService

from bqrequest import BlpapiServiceClient

import logging

_logger = logging.getLogger(__name__)


class BlpapiBregService(BregService):

    def __init__(self, bqapi_session=None):

        import bqapi

        session = bqapi_session or bqapi.get_session_singleton()
        self._service = BlpapiServiceClient('//blp/bqgwsvc',
                                            bqapi_session=session)

    def _build_request(self, accessor_name, entry_id, breg_type):
        return BregService._make_request_payload(
            accessor_name, entry_id, breg_type)

    def evaluate_breg(self, accessor_name, entry_id, breg_type):
        req = self._build_request(accessor_name, entry_id, breg_type)
        resps = self._service.send_request(operation='GwRequest', request=req)

        if len(resps) != 1:
            _logger.error("Unexpected response from breg servce: %s", resps)
            raise RuntimeError("Unexpected response from breg service")

        resp = resps[0]

        return BregService._get_value_from_response(resp)
