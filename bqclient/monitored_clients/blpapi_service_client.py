import hashlib
import logging
import re
import sys
from copy import deepcopy
from typing import Any, Callable, Dict, Mapping, Optional

from bqapi.promise import Promise

import bqmonitor
from bqclient.blpapi_service_client import (
    BlpapiServiceClient as _BlpapiServiceClient,
)
from bqmonitor.monitor import ReplayError, ReplayKeyNotFoundError
from bqmonitor.monitor_client_wrapper import (
    FAIL_ON_REPLAY_NOT_FOUND,
    MonitorClientWrapper,
)
from bqmonitor.types import JSONSerializableType

_logger = logging.getLogger(__name__)


class BlpapiServiceClient(MonitorClientWrapper, _BlpapiServiceClient):
    """
    Description
    -----------
    A monitored implementation of the BlpapiServiceClient. Publishes requests to
    the BlpapiServiceClient as they come in and replays responses from the
    BlpapiServiceClient in the case of replay mode.

    Also See
    --------
    >>> bqclient.BlpapiServiceClient
    """

    @property
    def service_name(self) -> str:
        return self._service_name

    @staticmethod
    def replay_key_for_body(body: object) -> str:
        """
        Description
        -----------
        Replaces the non-idempotent fields from the BlpapiRequestExecutor with None
        then returns a UID for the object. If the request was not called from the
        request executor, then we assume there are no non-idempotent fields.

        Returns
        -------
        str :
            The sha256 encoding of the request body
        """

        def replace_macros_with_generic(query: str) -> str:
            return re.sub(r"#_t\d+", "#_t0", query)

        if isinstance(body, Mapping):
            body_copy: Dict[str, Dict[Any, Any]] = deepcopy(dict(body))
        else:
            _logger.warning(
                "Request was passed unexpected type. Expected %s but was given %s",
                dict.__class__,
                type(body),
            )
            return MonitorClientWrapper.replay_key_for_body(
                replace_macros_with_generic(str(body))
            )
        try:
            body_copy["clientContext"]["clientRequestId"] = None
            body_copy["clientContext"]["timestamp"] = None
            body_copy["clientContext"]["extraMarkers"] = None
            if "timestamp" in body_copy:
                body_copy["timestamp"] = None
        except KeyError as e:
            _logger.info(
                "Request %s to have key '%s' but it was not found", body, e
            )

        return MonitorClientWrapper.replay_key_for_body(
            replace_macros_with_generic(str(body_copy))
        )

    def send_request(
        self,
        operation: str,
        request: JSONSerializableType,
        callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
    ) -> Any:
        """
        Description
        -----------
        The method called by BqapiRequestExecutor upon issuing a request. This
        is the entry point for the wrapper over BlpapiServiceClient. This
        logs requests and responses through the BlpapiServiceClient as
        Calls to bqmonitor if monitoring is enabled otherwise,
        it just requests data to/from the BlpapiServiceClient.

        Parameters
        ----------
        operation : str
            the kind of operation being sent to the outbound service, for
            instance bqlAsyncGetPayloadRequest

        request : dict
            the body of the request sent to the service

        callback : Callable, optional(default = None)
            an optional function called upon completion of an async request
            (request is assumed sync if not supplied)

        error_callback : Callable, optional(default = None)
            an optional function called upon error of an async request

        Returns
        -------
        Union[DictElement, Promise] :
            the response from the call to the blpapi service, either a dict of
            the service response or a promise
        """
        if not bqmonitor.monitoring_enabled():
            return super().send_request(
                operation,
                request,
                callback=callback,
                error_callback=error_callback,
            )
        if bqmonitor.mode() is bqmonitor.modes.REPLAY:
            try:
                replay_response = bqmonitor.replay_request(
                    self.replay_key_for_body(request)
                )
                if not replay_response:
                    raise ReplayError(
                        "Attempted to replay a request before it was finished"
                    )
            except ReplayKeyNotFoundError:
                if FAIL_ON_REPLAY_NOT_FOUND:
                    raise
                _logger.warning(
                    (
                        "The request:\n%s\n with replay key \n%s\n was not found",
                        "in the submitted replay jobs %s. Retrieving data from %s.",
                    ),
                    request,
                    self.replay_key_for_body(request),
                    [j.job_id for j in bqmonitor.jobs()],
                    self.service_name,
                )
                return super().send_request(
                    operation,
                    request,
                    callback=callback,
                    error_callback=error_callback,
                )
            except ReplayError:
                raise
            except Exception:
                if error_callback:
                    p = Promise(self._session.event_loop)
                    p.set_exc_info(sys.exc_info())
                    p.set_result(error_callback(sys.exc_info()))
                    return p
                else:
                    raise
            if callback:
                p = Promise(self._session.event_loop)
                p.set_result(callback(replay_response))
                return p
            return replay_response

        serializable_request = self.body_to_serializable_form(request)

        call = bqmonitor.Call(
            request=bqmonitor.Payload(body=serializable_request),
            service_name=self._service_name,
        )
        call.replay_key = self.replay_key_for_body(call.request.body)

        bqmonitor.publish(call)

        callback = bqmonitor.monitor_client_wrapper.callback_wrapper(
            call, callback, self.body_to_serializable_form
        )
        error_callback = bqmonitor.monitor_client_wrapper.callback_wrapper(
            call, error_callback, self.body_to_serializable_form
        )

        response = super().send_request(
            operation,
            request,
            callback=callback,
            error_callback=error_callback,
        )

        # if there's no callback we assume we are making a synchronous call so
        # we mark the request complete and log the response
        if not callback:
            call.response = bqmonitor.Payload(
                body=self.body_to_serializable_form(response)
            )
            # we republish the call once we have a response to overwrite the saved
            # version with the version with the response
            bqmonitor.publish(call)
        return response
