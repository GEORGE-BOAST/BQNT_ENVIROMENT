# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import logging
import six
import sys
import time
import functools
import copy

from .client_context import ClientContextBuilder
from .job import Job
from .response import Response
from .response_error import ResponseError

_logger = logging.getLogger(__name__)


# This class wraps a request payload to BQL with identifying information that
# is useful for logging purposes.
class _DecoratedRequest(object):
    def __init__(self, request, payload_id_mapper):
        self.__request = request
        self.__query_string = request.get('expression')
        client_context = request.get('clientContext', {})
        request_id = client_context.get('clientRequestId')
        payload_id = request.get('payloadId')
        # These are some design invariants that we can check on the request
        # that we built.
        if self.__query_string is not None:
            # This is a request to execute() or submit().
            assert request_id is not None
            assert payload_id is None
        else:
            # This is a request to fetch().  Look up info about the query sent
            # through submit.
            assert request_id is None
            assert payload_id is not None
            (request_id, self.__query_string) = payload_id_mapper.get_once(
                payload_id)
        assert self.__query_string is not None
        self.__id = f'request_id={request_id} payload_id={payload_id}'

    @property
    def id(self):
        return self.__id

    @property
    def query_string(self):
        # This should be used for display purposes only: it corresponds to the
        # query string associated with this request.
        return self.__query_string

    @property
    def request(self):
        return self.__request


class _PayloadIdMapper(object):
    def __init__(self):
        # For Async BQL requests: {payload_id: (request_id, query_string}.
        # This lets us look up the original request if we have the payload_id
        # for fetch().
        self.__payload_id_to_request = {}

    def add(self, payload_id, request_id, query_string):
        _logger.info('associating payload_id=%s with request_id=%s',
                      payload_id, request_id)
        self.__payload_id_to_request[payload_id] = (request_id, query_string)

    def get_once(self, payload_id):
        if payload_id in self.__payload_id_to_request:
            info = self.__payload_id_to_request[payload_id]
            # Once we look up info for a payload_id, delete it from the map
            # so the map doesn't grow indefinitely as submit() is called.
            del self.__payload_id_to_request[payload_id]
        else:
            # There is no request associated with this payload_id, so return
            # an empty struct.
            info = _PayloadIdMapper.__missing_info()
        return info

    @staticmethod
    def __missing_info():
        missing_request_id = None
        missing_query_string = ''
        return missing_request_id, missing_query_string


class _LoggedRequestSender(object):
    # Encapsulate the heart of RequestExecutor.__route_request() that logs info
    # about the request is being sent.  The main methods in this class allow
    # you to send either a synchronous or asynchronous request (from the
    # API standpoint of how the BAS service is called, not from the BQL
    # standpoint of Sync vs. Async BQL).
    def __init__(self, action, decorated_request, execution_context):
        self.__action = action
        self.__request = decorated_request.request
        self.__query_string = decorated_request.query_string
        self.__execution_context = execution_context
        self.__start_time = time.time()
        self.__id = decorated_request.id
        self.__log_execution()

    def async_send(self, function, callback, error_callback):
        # Add logging to the provided callbacks.
        logged_callback_success = self.__wrap_callback_logging(callback)
        logged_callback_failure = self.__wrap_error_callback_logging(
            error_callback)
        try:
            promise = function(self.__request,
                               callback=logged_callback_success,
                               error_callback=logged_callback_failure,
                               execution_context=self.__execution_context)
        except Exception as ex:
            # There was an error in attempting to send the request.  An error
            # response # from the server will be logged through the
            # logged_callback_failure callback.
            self.__log_failure(ex)
            raise ex
        else:
            # We only have a promise at this point.  A successful response
            # will be logged through the logged_callback_success callback.
            pass
        return promise

    def sync_send(self, function):
        try:
            response = function(self.__request, self.__execution_context)
        except Exception as ex:
            self.__log_failure(ex)
            raise ex
        else:
            self.__log_success()
        return response

    def __wrap_callback_logging(self, callback):
        if callback is not None:
            def logged_callback_success(resp):
                self.__log_success()
                return callback(resp)
        else:
            logged_callback_success = None
        return logged_callback_success

    def __wrap_error_callback_logging(self, error_callback):
        # wrap error callback with timing + logging
        def logged_callback_failure(exc_info):
            self.__log_failure(exc_info[1])
            if error_callback is not None:
                return error_callback(exc_info)
            else:
                six.reraise(exc_info[0], exc_info[1], exc_info[2])

        return logged_callback_failure

    def __log_execution(self):
        _HEAD_LENGTH = 4 * 1024
        _TAIL_LENGTH = 64
        if len(self.__query_string) > _HEAD_LENGTH + _TAIL_LENGTH:
            message = f"%s %s %.{_HEAD_LENGTH}s [... skipped ...] %s"
            _logger.info(message,
                         self.__id,
                         self.__action,
                         self.__query_string,  # log head...
                         self.__query_string[-_TAIL_LENGTH:])  # and tail
            # Log the entire request in debug level.
            _logger.debug("%s %s unabridged request was %s",
                          self.__id,
                          self.__action,
                          self.__query_string)
        else:
            _logger.info("%s %s: %s",
                         self.__id,
                         self.__action,
                         self.__query_string)

    def __log_success(self):
        # We do not log the actual request here as it was logged from
        # log_request_execution
        _logger.info("%s %s completed in %.3fs",
                     self.__id,
                     self.__action,
                     time.time() - self.__start_time)

    def __log_failure(self, exception):
        _logger.info(
            '%s %s failed request: "%s" error_external="%s" took %.3fs.',
            self.__id,
            self.__action,
            self.__request,
            exception,
            time.time() - self.__start_time
        )
        if not isinstance(exception, ResponseError) or exception.log_error:
            _logger.error("BQL ERROR: %s, %s", exception, self.__id)


class StringRequestExecutor(object):
    _retryable_exceptions = ()

    def __init__(self):
        self.__payload_id_mapper = _PayloadIdMapper()

    def retryable_exceptions(self):
        return self._retryable_exceptions

    def execute_string(self, request_string,
                       callback=None, error_callback=None):
        """Executes the given BQL query request string and return its response.

        Returns a :class:`Response` instance or raises :class:`ResponseError`
        if the backend could not process the request.
        """
        return self._execute_string(request_string,
                                    item_names=None,
                                    callback=callback,
                                    error_callback=error_callback)

    def fetch(self, job, callback=None, error_callback=None,
              is_large_req=False, num_chunks=1):
        """Fetches results/status of a previously submitted job"""
        return self._fetch(job,
                           callback,
                           error_callback,
                           is_large_req=is_large_req,
                           num_chunks=num_chunks
                           )

    def submit_string(self,
                      request_string,
                      callback=None,
                      error_callback=None):
        """Schedules the given BQL query request string and returns payload id.

        Returns a :class:`Job` instance or raises :class:`ResponseError`
        if the backend could not process the request.
        If callback is provided, a Promise is returned.
        """
        return self._submit_string(request_string,
                                   item_names=None,
                                   callback=callback,
                                   error_callback=error_callback)

    def _demarshal_response(self, request_type, partial_response):
        raise NotImplementedError()

    def _send_execute_request(self, request):
        raise NotImplementedError()

    def _send_execute_request_async(self, request, callback, error_callback):
        raise NotImplementedError()

    def _send_submit_request(self, request):
        raise NotImplementedError()

    def _send_submit_request_async(self, request, callback, error_callback):
        raise NotImplementedError()

    def _send_fetch_request(self, request):
        raise NotImplementedError()

    def _send_fetch_request_async(self, request, callback, error_callback):
        raise NotImplementedError()

    def _execute_impl(self, request, execution_context=None):
        # Execute the given request and return a PyBQL Response object
        # request is a dict containing 'expression' and 'clientContext'
        # elements.
        partial_responses = self._send_execute_request(request)
        json_fragments = [self._demarshal_response('execute', r)
                          for r in partial_responses]
        return Response.from_execute_response(json_fragments, execution_context)

    def _execute_async_impl(self,
                            request,
                            callback,
                            error_callback=None,
                            execution_context=None):
        # Execute the given request and return a promise whose result
        # is the return value of the callback or the error_callback.
        # Schedule the given request and return a promise whose result
        # is the return value of the callback or the error_callback.
        #
        # request is a dict containing 'expression' and 'clientContext'
        # elements.
        def cb_wrapper(partial_responses):
            try:
                _logger.debug("Received %d responses: %s",
                              len(partial_responses), partial_responses)
                json_fragments = [self._demarshal_response('execute', r)
                                  for r in partial_responses]
                response = Response.from_execute_response(json_fragments,
                                                          execution_context)
            except Exception:
                _logger.debug('request: %s, error occurs for '
                              '_execute_async_impl cb_wrapper()',
                              request)
                if error_callback is not None:
                    return error_callback(sys.exc_info())
                else:
                    raise
            return callback(response)

        return self._send_execute_request_async(request,
                                                cb_wrapper,
                                                error_callback)

    def _submit_impl(self, request, execution_context=None):
        # Schedule the given request and return a pybql Job object.  request is
        # a dict containing 'expression' and 'clientContext' elements.
        partial_responses = self._send_submit_request(request)
        response = self._demarshal_response('submit', partial_responses[0])
        return Job.from_submit_response(response, execution_context)

    def _submit_async_impl(self,
                           request,
                           callback,
                           error_callback=None,
                           execution_context=None):
        # Schedule the given request and return a promise whose result
        # is the return value of the callback or the error_callback.
        # request is a dict containing 'expression' and 'clientContext'
        # elements.
        def cb_wrapper(partial_responses):
            try:
                _logger.debug("Received %d responses: %s",
                              len(partial_responses), partial_responses)
                response = self._demarshal_response('submit',
                                                    partial_responses[0])
                job = Job.from_submit_response(response, execution_context)
            except Exception:
                _logger.debug('request: %s, error occurs for '
                              '_submit_async_impl cb_wrapper()',
                              request)
                if error_callback is not None:
                    return error_callback(sys.exc_info())
                else:
                    raise
            return callback(job)

        return self._send_submit_request_async(request,
                                               cb_wrapper,
                                               error_callback)

    def _fetch_impl(
            self, request, execution_context=None,
            is_large_req=False, num_chunks=1):
        json_fragments = []
        if is_large_req:
            start = end = 1
            res, payload_info = self._send_request_and_process_partial_response(
                request, start, end
            )
            json_fragments.extend(res)
            # For the first response, get the total num of chunks
            # from partial payload info element
            total_chunks = payload_info['totalPayloadChunks']
            while end < total_chunks:
                start = end + 1
                end = min(start + num_chunks - 1, total_chunks)
                res, _ = self._send_request_and_process_partial_response(
                    request, start, end
                )
                json_fragments.extend(res)
        else:
            partial_responses = self._send_fetch_request(request)
            _logger.debug("Partial responses without chunking: %s"
                          % list(partial_responses))
            json_fragments = [self._demarshal_response('fetch', r)
                              for r in partial_responses]
        return Response.from_fetch_response(json_fragments, execution_context)

    def _send_request_and_process_partial_response(self, request, start, end):
        request = self._add_payload_range_info(request, start, end)
        _logger.debug("Partial Response requested with start index : %d, "
                      "end index %d " % (start, end))
        response = self._send_fetch_request(request)
        partial_responses = []
        num_chunks = end - start + 1
        if len(response) != num_chunks:
            raise ResponseError("Expected %d partial responses, Got %d."
                                % (num_chunks, len(response)))
        payload_info = None
        for i in range(num_chunks):
            json_fragment = self._demarshal_response(
                'fetch_large', response[i])
            if 'bqlAsyncPayloadResponse' not in json_fragment:
                raise ResponseError("Async fetch response doesn't contain "
                                    "'bqlAsyncPayloadResponse.'")
            if 'partialPayloadInfo' not in json_fragment:
                raise ResponseError(
                    "Expected 'partialPayloadInfo' in the response, not found.")
            payload_info = json_fragment['partialPayloadInfo']
            res = json_fragment['bqlAsyncPayloadResponse']

            partial_responses.append(res)
        return partial_responses, payload_info

    def _fetch_async_impl(self,
                          request,
                          callback,
                          error_callback=None,
                          execution_context=None,
                          is_large_req=False,
                          num_chunks=1):
        if is_large_req:
            json_fragments = []

            # This function decides what to do after processing chunks [start,
            # end].  If that's the last of the chunks, then we form a Response
            # from all the chunks accumulated in json_fragments; if not, then
            # we send a request to pull the next sequence of chunks.
            def after_chunk(start, end, total_num_chunks):
                try:
                    if end < total_num_chunks:
                        start = end + 1
                        end = min(start + num_chunks - 1, total_num_chunks)
                        next_request = self._add_payload_range_info(request,
                                                                    start, end)
                        _logger.info('request: % s, '
                                     'after_chunk: fetching next piece %s',
                                     request, next_request)
                        next_chunk_func = functools.partial(
                            on_remaining_chunks,
                            start=start,
                            end=end,
                            total_num_chunks=total_num_chunks)
                        return self._send_fetch_request_async(next_request,
                                                              next_chunk_func,
                                                              error_callback)
                    else:
                        _logger.info('request: %s, '
                                     'after_chunk: received all chunks',
                                     request)
                        response = Response.from_fetch_response(
                            json_fragments,
                            execution_context
                        )
                        # Execute callback on the response outside the
                        # try/except block.
                except Exception:
                    _logger.debug('request: %s, '
                                  'error occurs for after_chunk()', request)
                    if error_callback is not None:
                        return error_callback(sys.exc_info())
                    else:
                        raise
                return callback(response)

            # This is the callback that executes when a chunk response comes
            # back for all but the first request.  In this case we already
            # know the total number of chunks that need to be pulled in the
            # overall Response, and the job here is to send out the next
            # request for chunks through the after_chunk() function.
            def on_remaining_chunks(partial_responses,
                                    start,
                                    end,
                                    total_num_chunks):
                try:
                    for partial_response in partial_responses:
                        json_fragment = self._demarshal_response(
                            'fetch_large',
                            partial_response
                        )
                        json_fragments.append(
                            json_fragment['bqlAsyncPayloadResponse'])
                except Exception:
                    _logger.debug('request: %s, '
                                  'error occurs for on_remaining_chunks()',
                                  request)
                    if error_callback is not None:
                        return error_callback(sys.exc_info())
                    else:
                        raise
                return after_chunk(start, end, total_num_chunks)

            # This is the callback that executes when we receive the response
            # from the first request.  The first request is always for a single
            # chunk, and from that response we can tell how many remaining chunks
            # there are in the overall Response.
            def on_first_chunk(partial_responses):
                try:
                    # For the first request with start=1 and end=1, we expect
                    # only one partial response returned.  But it's different
                    # from the later requests in that we need to pull
                    # totalPayloadChunks to know how many chunks remain.
                    assert len(partial_responses) == 1
                    start = end = 1
                    json_fragment = self._demarshal_response(
                        'fetch_large',
                        partial_responses[0]
                    )
                    payload_info = json_fragment['partialPayloadInfo']
                    total_num_chunks = payload_info['totalPayloadChunks']
                    _logger.info('on_first_chunk: total_num_chunks=%s',
                                 total_num_chunks)
                    json_fragments.append(
                        json_fragment['bqlAsyncPayloadResponse'])
                except Exception:
                    if error_callback is not None:
                        return error_callback(sys.exc_info())
                    else:
                        raise
                return after_chunk(start, end, total_num_chunks)

            first_request = self._add_payload_range_info(request, start=1,
                                                         end=1)
            _logger.debug('first_request: %s', first_request)
            return self._send_fetch_request_async(first_request,
                                                  on_first_chunk,
                                                  error_callback)
        else:
            # Return a promise whose result is the return value of either the
            # callback executed on the BQL response for request['payloadId'] or
            # error_callback.
            def cb_wrapper(partial_responses):
                try:
                    _logger.debug("Received %d responses: %s",
                                  len(partial_responses), partial_responses)
                    fragments = [self._demarshal_response('fetch', r)
                                 for r in partial_responses]
                    response = Response.from_fetch_response(fragments,
                                                            execution_context)
                except Exception:
                    if error_callback is not None:
                        return error_callback(sys.exc_info())
                    else:
                        raise
                return callback(response)

            return self._send_fetch_request_async(request,
                                                  cb_wrapper,
                                                  error_callback)

    def _fetch(self,
               job,
               callback=None,
               error_callback=None,
               is_large_req=False,
               num_chunks=1
               ):
        request = self.__make_decorated_request({'payloadId': job.payload_id})
        fetch_impl_func = functools.partial(
            self._fetch_impl, is_large_req=is_large_req, num_chunks=num_chunks)
        fetch_async_impl_func = functools.partial(
            self._fetch_async_impl, is_large_req=is_large_req,
            num_chunks=num_chunks)
        return StringRequestExecutor.__route_request('fetch',
                                                     request,
                                                     job.execution_context,
                                                     fetch_impl_func,
                                                     fetch_async_impl_func,
                                                     callback,
                                                     error_callback)

    def _get_client_context_builder(self):
        # Derived classes can override this function to provide their own way to
        # build clientContext structs.  By default we use the one below for all
        # Requests.
        return ClientContextBuilder()

    def _execute_string(self,
                        request_string,
                        item_names,
                        callback=None,
                        error_callback=None):
        client_context = self._get_client_context_builder().get_context()
        request = self.__make_decorated_request({
            'expression': request_string,
            'clientContext': client_context
        })
        request_id = client_context.get('clientRequestId')
        execution_context = {'item_names': item_names, 'request_id': request_id}
        return StringRequestExecutor.__route_request('execute',
                                                     request,
                                                     execution_context,
                                                     self._execute_impl,
                                                     self._execute_async_impl,
                                                     callback,
                                                     error_callback)

    def _submit_string(self,
                       request_string,
                       item_names,
                       callback=None,
                       error_callback=None):
        client_context = self._get_client_context_builder().get_context()
        request = {
            'expression': request_string,
            'clientContext': client_context
        }
        decorated_request = self.__make_decorated_request(request)
        execution_context = {'item_names': item_names}
        if callback is not None:
            def cb_wrapper(job):
                # Remember the request_id associated with the returned
                # payload_id.
                self.__register_payload_id(job.payload_id, request)
                # cb_wrapper only executes if job is not None; otherwise,
                # control flows through self.__submit_impl().
                return callback(job)
        else:
            cb_wrapper = None

        return StringRequestExecutor.__route_request('submit',
                                                     decorated_request,
                                                     execution_context,
                                                     self.__submit_impl,
                                                     self._submit_async_impl,
                                                     cb_wrapper,
                                                     error_callback)

    @staticmethod
    def __route_request(action,
                        request,
                        execution_context,
                        sync_function,
                        async_function,
                        callback=None,
                        error_callback=None):
        # This function is a single entry point for sending a request either
        # synchronously or asynchronously.  The implementations for sending
        # the particular request synchronously or asynchronously are provided
        # by sync_function and async_function, respectively.  The sync_function
        # is chosen if a callback is not provided, and the synchronous response
        # is returned.  If a callback is provided, then the async_function is
        # executed to send the request asynchronously, returning a promise that
        # is resolved to either the callback (to process a successfully-returned
        # response) or error_callback (to process an exception that was raised).
        sender = _LoggedRequestSender(action, request, execution_context)
        if callback is not None:
            promise = sender.async_send(async_function,
                                        callback,
                                        error_callback)
            result = promise
        else:
            response = sender.sync_send(sync_function)
            result = response
        return result

    def __make_decorated_request(self, request):
        return _DecoratedRequest(request, self.__payload_id_mapper)

    def __submit_impl(self, request, execution_context):
        job = self._submit_impl(request, execution_context)
        self.__register_payload_id(job.payload_id, request)
        return job

    def __register_payload_id(self, payload_id, submit_request):
        request_id = submit_request['clientContext']['clientRequestId']
        query_string = submit_request['expression']
        self.__payload_id_mapper.add(payload_id, request_id, query_string)

    def _add_payload_range_info(self, request, start, end):
        new_request = copy.copy(request)
        payload_range = {'startIndex': start, 'endIndex': end}
        new_request['partialPayloadRange'] = payload_range
        return new_request
