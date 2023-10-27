import hashlib
import logging
import re
from concurrent.futures import Future
from copy import deepcopy
from typing import Any, Dict, List, Mapping, Union

import bqmonitor
from bqclient import FutureExt
from bqclient.http_service_client import _DEFAULT_USER_CONTEXT
from bqclient.http_service_client import (
    HttpServiceClient as _HttpServiceClient,
)
from bqmonitor import ReplayError, ReplayKeyNotFoundError
from bqmonitor.monitor_client_wrapper import (
    FAIL_ON_REPLAY_NOT_FOUND,
    MonitorClientWrapper,
)

_logger = logging.getLogger(__name__)


class HttpServiceClient(MonitorClientWrapper, _HttpServiceClient):
    @property
    def service_name(self) -> str:
        return self._service_info.service_name

    @staticmethod
    def replay_key_for_body(body: object) -> str:
        """
        Description
        -----------
        Given the request body, this method returns the key used to identify
        the request in the case of a replay.

        Replaces the non-idempotent fields from the HTTPRequestExecutor with None
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
            body_copy["query"]["clientContext"]["clientRequestId"] = None
            body_copy["query"]["clientContext"]["timestamp"] = None
            if "timestamp" in body_copy["query"]:
                body_copy["query"]["timestamp"] = None
        except KeyError as e:
            _logger.info(
                "Request %s to have key '%s' but it was not found", body, e
            )
        return MonitorClientWrapper.replay_key_for_body(
            replace_macros_with_generic(str(body_copy))
        )

    def send_request(
        self,
        request,
        user_context=_DEFAULT_USER_CONTEXT,
        callback=None,
        error_callback=None,
        timeout=None,
    ) -> Union[List[Any], FutureExt]:

        if not bqmonitor.monitoring_enabled():
            return super().send_request(
                request=request,
                user_context=user_context,
                callback=callback,
                error_callback=error_callback,
                timeout=timeout,
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
                    request=request,
                    user_context=user_context,
                    callback=callback,
                    error_callback=error_callback,
                    timeout=timeout,
                )
            except ReplayError:
                raise
            except Exception as e:
                if error_callback:
                    f = FutureExt(Future())
                    f.set_exception(e)
                    f.set_result(error_callback(sys.exc_info()))
                    return f
                else:
                    raise
            if callback:
                f = FutureExt(Future())
                f.set_result(callback(replay_response))
                return f
            return replay_response

        serializable_request = self.body_to_serializable_form(request)
        call = bqmonitor.Call(
            request=bqmonitor.Payload(body=serializable_request),
            service_name=self.service_name,
            replay_key=self.replay_key_for_body(request),
        )

        bqmonitor.publish(call)

        callback = bqmonitor.monitor_client_wrapper.callback_wrapper(
            call, callback, self.body_to_serializable_form
        )

        error_callback = bqmonitor.monitor_client_wrapper.callback_wrapper(
            call, error_callback, self.body_to_serializable_form
        )

        response = super().send_request(
            request=request,
            user_context=user_context,
            callback=callback,
            error_callback=error_callback,
            timeout=timeout,
        )

        if not callback:
            call.response = bqmonitor.Payload(
                body=self.body_to_serializable_form(response)
            )
            bqmonitor.publish(call)

        return response
