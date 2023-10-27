from collections import deque, namedtuple
import sys

import logging

_logger = logging.getLogger(__name__)

AsyncRequest = namedtuple('AsyncRequest',
                          [
                              'operation',
                              'request',
                              'callback',
                              'error_callback',
                              'promise'
                          ])


class BlpapiServiceClient(object):
    def __init__(self,
                 service_name,
                 max_concurrent_requests=10,
                 bqapi_session=None):

        import bqapi

        self._service_name = service_name
        self._session = bqapi_session or bqapi.get_session_singleton()

        # Async requests bookkeeping
        self._wait_queue = deque()
        self._num_pending_requests = 0
        self._max_concurrent_requests = max_concurrent_requests

    @property
    def max_concurrent_requests(self):
        return self._max_concurrent_requests

    @property
    def num_pending_requests(self):
        return self._num_pending_requests

    @property
    def at_max_concurrent_requests(self):
        return self.num_pending_requests == self.max_concurrent_requests

    @property
    def num_waiting_requests(self):
        return len(self._wait_queue)

    def _execute_send_request(self,
                              operation,
                              request,
                              callback=None,
                              error_callback=None):

        import bqapi

        return self._session.send_request(
            self._service_name,
            operation,
            request,
            bqapi.format.SimpleFormat(),
            callback=callback,
            error_callback=error_callback)

    def _on_callback_invoked(self):
        assert self._num_pending_requests > 0
        self._num_pending_requests -= 1
        self._trigger_waiting_requests()

    def _trigger_waiting_requests(self):
        """
        Pick up jobs from the wait queue and submit them for execution.
        """
        while (self._wait_queue and not self.at_max_concurrent_requests):
            request = self._wait_queue.pop()

            def set_promise_result(result):
                request.promise.set_result(result)

            def error_handler(exc_info):
                request.promise.set_exc_info(exc_info)

            self._send_request_async(
                request.operation,
                request.request,
                request.callback,
                request.error_callback).then(set_promise_result,
                                             error_handler)

    def _send_request_async(self,
                            operation,
                            request,
                            callback,
                            error_callback=None):

        from bqapi import Promise

        assert callback is not None
        assert self.num_pending_requests <= self.max_concurrent_requests

        # If we've reached the maximum number of concurrent requests,
        # queue up the request until such time that we can execute
        # additional requests. Return a promise whose result will be
        # the return value of the user provided callback.

        if self.at_max_concurrent_requests:
            promise = Promise(self._session.event_loop)
            self._wait_queue.appendleft(
                AsyncRequest(operation=operation,
                             request=request,
                             callback=callback,
                             error_callback=error_callback,
                             promise=promise))
            return promise

        def error_cb_wrapper(exc_info):
            self._on_callback_invoked()
            if error_callback is not None:
                return error_callback(exc_info)
            else:
                raise exc_info[1].with_traceback(exc_info[2])

        def cb_wrapper(message):
            try:
                self._on_callback_invoked()
                responses = [resp.element() for resp in message]
                _logger.debug("Received %d responses: %s",
                              len(responses), responses)
            except Exception:
                if error_callback is not None:
                    return error_callback(sys.exc_info())
                else:
                    raise

            return callback(responses)

        assert self._num_pending_requests < self._max_concurrent_requests

        self._num_pending_requests += 1
        return self._execute_send_request(operation,
                                          request,
                                          cb_wrapper,
                                          error_cb_wrapper)

    def send_request(self,
                     operation,
                     request,
                     callback=None,
                     error_callback=None):

        if callback is not None:
            return self._send_request_async(operation,
                                            request,
                                            callback,
                                            error_callback)

        # synchronous request
        msg = self._execute_send_request(operation, request)
        return [resp.element() for resp in msg]
