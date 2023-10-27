import hashlib
import os
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Callable, Iterable, Mapping, Optional, Union

import bqmonitor
from bqmonitor.call import Call
from bqmonitor.payload import ErrorResponse
from bqmonitor.types import JSONSerializableType

FAIL_ON_REPLAY_NOT_FOUND = bool(
    int(os.environ.get("FAIL_ON_REPLAY_NOT_FOUND", False))
)


def callback_wrapper(
    call: Call,
    callback: Optional[Callable],
    serialize: Callable[[Any], JSONSerializableType],
) -> Optional[Callable]:
    """
    Description
    -----------
    A method which wraps a given callback with the _internal method.
    The _internal method records the response information coming into the
    callback to the given call object.

    Parameters
    ----------
    call : call -
        the call which this callback refers to, that is the call's
        call.request.body that triggered this callback

    callback : Callable, optional -
        the callback which we are wrapping

    serialize: Callable[[Any], JSONSerializableType] - a method taking the input
    of the result of a request to the wrapped service and returning a serializable
    version of said result

    Returns
    -------
    Optional[Callable] -
        the wrapped callback which logs the response to the call
        when it is called, or None if no callback was supplied
    """
    if not callback:
        return None

    def _internal(response):
        call.response = bqmonitor.Payload(body=serialize(response))
        # we republish the call once we have a response to overwrite the saved
        # version with the version with the response
        bqmonitor.publish(call)
        return callback(response)

    return _internal


class MonitorClientWrapper(ABC):
    """
    Description
    -----------
    A Wrapper class to be implemented with a client which allows the client to
    publish requests to bqmonitor and replay from bqmonitor

    Attributes
    ----------
    service_name: str
        the name of the service the monitor wraps
    """

    @staticmethod
    def known_invalid_type(body: object) -> bool:
        """
        Description
        -----------
        For the response for our service, this method determines If the object
        is a known response type which cannot be serialized.

        Returns
        -------
        bool :
            if the method response cannot be serialized
        """
        # In an error response, bqapi will raise sys.exc_info() with the error.
        # This is a tuple(<Type[error]>, error, <error_stack_trace>). We cannot
        # serialize the stack trace or the error type
        if isinstance(body, (type, TracebackType)):
            return True
        return False

    @classmethod
    def body_to_serializable_form(
        cls, body: object
    ) -> Union[JSONSerializableType, ErrorResponse]:
        """
        Description
        -----------
        Given the response from the client, this method
        translates the response to a json serializable type ie:

        >>> bqmonitor.types.JSONSerializableType

        This is needed to ensure we can store responses from the service in our
        store of choice.

        Parameters
        ----------
        body: object
            The response object from the service which we make serializable

        Returns
        -------
        JSONSerializableType :
            A serializable version of the object.
        """
        if cls.known_invalid_type(body):
            return None
        if isinstance(body, BaseException):
            return ErrorResponse.from_exception(body).to_dict()
        if isinstance(body, Mapping):
            return {
                cls.body_to_serializable_form(
                    k
                ): cls.body_to_serializable_form(v)
                for k, v in body.items()
            }
        if isinstance(body, Iterable) and not isinstance(body, str):
            return [cls.body_to_serializable_form(v) for v in body]
        return body

    @staticmethod
    def replay_key_for_body(body: object) -> str:
        """
        Description
        -----------
        Returns a unique ID for the object.

        Returns
        -------
        str :
            The sha256 encoding of the request body
        """
        return hashlib.sha256(str(body).encode("utf-8")).hexdigest()

    @property
    @abstractmethod
    def service_name(self) -> str:
        """
        Returns
        -------
        str :
            The name of the service which the client reaches out to
        """
