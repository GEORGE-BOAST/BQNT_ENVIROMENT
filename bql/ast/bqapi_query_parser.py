# Copyright 2019 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from bqclient import BlpapiServiceClient
from .query_parser import QueryParser


class BqapiQueryParser(QueryParser):
    def __init__(self, bqapi_session=None):
        super(BqapiQueryParser, self).__init__()
        self._svc = BlpapiServiceClient(
            f'//blp/{self.svc_name}',
            bqapi_session=bqapi_session
        )

    def _send_request(self, request):
        return self._svc.send_request(self.request_name, request)
