# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.
from bqmonitor.permissions import is_enabled_for_bqmonitor

from .request_executor import RequestExecutor

_FETCH_CONCURRENCY = 5

class BqapiRequestExecutor(RequestExecutor):
    def __init__(self,
                 bqapi_session=None,
                 service_name=str('//blp/bqlsvc'),
                 max_concurrent_requests=10):
        super().__init__()
        if is_enabled_for_bqmonitor():
            from bqclient.monitored_clients import BlpapiServiceClient
        else:
            from bqclient import BlpapiServiceClient
        self._service = BlpapiServiceClient(
            service_name,
            max_concurrent_requests=max_concurrent_requests,
            bqapi_session=bqapi_session)
        self._bqlasvc_service = BlpapiServiceClient(
            '//blp/bqlasvc',
            max_concurrent_requests=min(_FETCH_CONCURRENCY,
                                        max_concurrent_requests),
            bqapi_session=bqapi_session)
        # Retryable exceptions must be instance properties because
        # bqapi not available at import time.
        import bqapi
        self._retryable_exceptions = (bqapi.RequestTimedOutError,)

    @property
    def max_concurrent_requests(self):
        return self._service.max_concurrent_requests

    @property
    def num_pending_requests(self):
        return self._service.num_pending_requests

    @property
    def at_max_concurrent_requests(self):
        return self._service.at_max_concurrent_requests

    @property
    def num_waiting_requests(self):
        return self._service.num_waiting_requests

    def _demarshal_response(self, request_type, response):
        return response

    def _send_execute_request(self, request):
        return self._service.send_request('sendQuery', request)

    def _send_execute_request_async(self, request, callback, error_callback):
        assert callback is not None
        return self._service.send_request('sendQuery',
                                          request,
                                          callback,
                                          error_callback)

    def _send_submit_request(self, request):
        return self._service.send_request('sendBqlAsyncQuery', request)

    def _send_submit_request_async(self, request, callback, error_callback):
        assert callback is not None
        return self._service.send_request('sendBqlAsyncQuery',
                                          request,
                                          callback,
                                          error_callback)

    def _send_fetch_request(self, request):
        return self._bqlasvc_service.send_request('bqlAsyncGetPayloadRequest',
                                                  request)

    def _send_fetch_request_async(self, request, callback, error_callback):
        assert callback is not None
        return self._bqlasvc_service.send_request('bqlAsyncGetPayloadRequest',
                                                  request,
                                                  callback,
                                                  error_callback)
