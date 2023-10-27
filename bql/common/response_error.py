# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.


class ResponseError(RuntimeError):
    # Raised when a BQL request causes an error.

    # For example invalid syntax of the query string, non-existing functions,
    # data items or securities, or other run-time errors.
    def __init__(self, exception_list, execution_context=None):
        # Initialize an :class:`ResponseError`.

        # The `exception_list` parameter is the "responseExceptions" field in
        # the response JSON.

        # Preserve the full list of messages for inspection by error
        # handlers.
        self.exception_messages = [exc['message']
                                   for exc in exception_list
                                   if 'message' in exc]
        self.internal_messages = exception_list

        self.log_error = True

        message_sub_categories = {
            exc['messageSubcategory'] for exc in self.internal_messages
            if 'messageSubcategory' in exc
        }
        if 'UNSUPPORTED_ROLLING_ANALYTICS' in message_sub_categories:
            self.log_error = False

        # `execution_context` is not a required parameter in the code base in
        # the functions it gets passed to, so it's possible for the
        # `execution_context` to be `None`.
        request_id = (execution_context.get('request_id') if execution_context
            is not None else None)
        self._request_id = request_id

        # The message on the exception object itself, though, is the
        # concatenation of all messages.
        if self.exception_messages:
            message = '\n'.join(self.exception_messages[-2:])
        else:
            message = 'Unknown error while processing BQL request'

        super(ResponseError, self).__init__(message)

    @property
    def request_id(self):
        return self._request_id


class UnexpectedResponseError(ValueError):
    # Raised when the response from the backend is different from
    # what was expected.

    # This indicates a mismatch between the BQL client library and the BQL
    # backend service.
    pass


class NotFoundError(Exception):
    # Raised when any part (payload id or item index) of the URL is invalid
    # It can also be used to indicate the end of chunks
    pass
