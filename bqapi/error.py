# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import


class SessionClosedError(Exception):

    """Raised if the session has been closed unexpectedly.

    This error is raised if a request cannot be completed because the
    session has been closed, or when a request cannot be sent because the
    session is closed already.
    """


class AuthorizationError(Exception):

    """Raised if authorization fails.

    This error is raised if authorization fails and no authorized
    identity can be obtained with the authorization options in the
    session's :class:`Settings`.
    """


class ServiceClosedError(Exception):

    """Raised if a service cannot be opened.

    This error is raised if a service cannot be opened, for example
    if there is no such service with the given name.
    """


class SubscriptionClosedError(Exception):

    """Raised if a subscription is terminated.

    This error is raised if a subscription cannot be initiated, for
    example because the topic string is incorrect, or if an existing
    subscription has been terminated.
    """


class RequestError(Exception):

    """Raised if the server could not process a request.

    This error is raised if a request was sent to the server, but its
    response indicates that the server could not process the request
    successfully, for example due to invalid input data.
    """


class RequestTimedOutError(RequestError):

    """Raised if a request times out instead of being processed.

    This is raised if the REQUEST_STATUS description in the response indicates
    'RequestFailure', which indicates that the request has timed out. (See
    page 73 of the BLPAPI Core Developer Guide).
    """


class SecurityError(RequestError):

    """Raised if a security does not exist.

    This error is raised if a security specified in a request to a
    Bloomberg service does not exist. This exception is derived from
    :class:`RequestError`.
    """


class FieldError(RequestError):

    """Raised if a field does not exist.

    This error is raised if a field specified in a request to a
    Bloomberg service does not exist. This exception is derived from
    :class:`RequestError`.
    """


class StudyError(Exception):

    """Raised if a technical study does not exist.

    This error is raised if a technical study was specified in a request
    to a Bloomberg service that does not exist.
    """
