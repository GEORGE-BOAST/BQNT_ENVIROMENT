import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from typing.io import IO

from bloomberg.cis.sdk import ServerModeClient

import bqmonitor
from bqclient.monitored_clients.cis.base_cis_client import BaseCISClient
from bqclient.monitored_clients.cis.types import BaseURL, HTTPMethod
from bqmonitor import ReplayError, ReplayKeyNotFoundError
from bqmonitor.monitor_client_wrapper import FAIL_ON_REPLAY_NOT_FOUND

_IS_BQNT_DESKTOP = False
try:
    import bloomberg.bquant.enterprise.auth as pets_auth
except ImportError:
    _IS_BQNT_DESKTOP = True

_logger = logging.getLogger(__name__)


class CISRestClient(BaseCISClient):
    def __init__(
        self,
        base_url: BaseURL,
        service_name: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        use_pets: bool = True,
        uuid: Optional[int] = None,
    ):
        if _IS_BQNT_DESKTOP and use_pets:
            raise ValueError(
                "Cannot use PETs on BQNT Desktop. Please provide `client_id` and `client_secret` and set use_pets to False"
            )
        super(CISRestClient, self).__init__(
            base_url, service_name, client_id, client_secret, use_pets
        )

        self._uuid = uuid
        self._server_mode_client = ServerModeClient(
            base_url.value, client_id, client_secret
        )

    @property
    def uuid(self) -> int:
        return self._uuid

    def _request(
        self,
        method: str,
        endpoint: str,
        body: Optional[Union[str, Dict[Any, Any], IO]] = None,
        query: Optional[Dict[Any, Any]] = None,
        nojwt: bool = False,
        jwtkvp: Optional[Dict[Any, Any]] = None,
        headers: Optional[List[Tuple[str, str]]] = None,
        stream: bool = False,
        verbosity: int = 0,
        follow_redirect: bool = True,
    ) -> Any:
        if query is None:
            query = {}
        if jwtkvp is None:
            jwtkvp = {}
        if headers is None:
            headers = []

        if self._use_pets:
            self._server_mode_client.urlfactory.jwt._last_jwt = (
                " "  # this is for a bug in the ServerModeClient lib
            )
            pets_headers = list(pets_auth.get_auth_headers().items())
            headers += pets_headers
            nojwt = True

        return self._server_mode_client.request(
            method=method,
            endpoint=endpoint,
            body=body,
            query=query,
            nojwt=nojwt,
            jwtkvp=jwtkvp,
            headers=headers,
            stream=stream,
            verbosity=verbosity,
            follow_redirect=follow_redirect,
        )

    def send_request(
        self,
        method: HTTPMethod,
        endpoint: str,
        body: Optional[Union[str, Dict[Any, Any], IO]] = None,
        query: Optional[Dict[Any, Any]] = None,
        nojwt: bool = False,
        jwtkvp: Optional[Dict[Any, Any]] = None,
        headers: Optional[List[Tuple[str, str]]] = None,
        stream: bool = False,
        verbosity: int = 0,
        follow_redirect: bool = True,
    ) -> Any:
        """
        Description
        -----------
        Make a request to the API Gateway this client is configured to use. This
        logs requests and responses through the CISRestClient as
        Calls to bqmonitor if monitoring is enabled otherwise,
        it just requests data to/from the CISRestClient

        Parameters
        ----------
        method : HTTPMethod
            Required The HTTP method to use. One of GET, POST, PUT, PATCH, DELETE

        endpoint: str
            Required The path of the endpoint to call. Should start with a "/" and NOT include any query parameters.

        body: Union[str, Dict[Any, Any], IO]
            Required if method is POST, PATCH or DELETE. If the body is a string type, it will be set as-is.
            If it is a Python dictionary type, it will be JSON encoded. If the body is a stream or file object, it's read()
            method will be called to load the data that will be sent.

        query: Dict[Any, Any]
            Optional Key-value dictionary of URI query parameters to include with the request.

        nojwt: bool
            Optional If True a JWT will not be created for the request.

        jwtkvp: Dict[Any, Any]
            Optional Additional claims to include in the JWT.

        headers: List[str]
            Optional Additional headers to include in the request.

        stream: bool
            Optional If True the response will not be read immediately. See the Advanced Usage documentation
            for the requests library to understand how to handle the response object.

        verbosity: int
            Optional If verbosity is greater than 0, this library will print() trace debugging information
            about the request and response. 0: No output, 1: print response status , 2: add request summary and response
            content, 3: add JWT details, and 4: add request and response headers.

        Returns
        -------
        Any :
            the response from the call to the endpoint
        """
        if query is None:
            query = {}
        if jwtkvp is None:
            jwtkvp = {}
        if headers is None:
            headers = []

        method = method.value
        if self.uuid is not None:
            jwtkvp.update({"uuid": self.uuid})
        if not bqmonitor.monitoring_enabled():
            return self._request(
                method=method,
                endpoint=endpoint,
                body=body,
                query=query,
                nojwt=nojwt,
                jwtkvp=jwtkvp,
                headers=headers,
                stream=stream,
                verbosity=verbosity,
                follow_redirect=follow_redirect,
            )

        request = {
            "method": method,
            "endpoint": endpoint,
            "body": body,
            "query": query,
        }
        if bqmonitor.mode() is bqmonitor.modes.REPLAY:
            try:
                replay_response = bqmonitor.replay_request(
                    self.replay_key_for_body(str(request))
                )
                if replay_response is None:
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
                    self.replay_key_for_body(str(request)),
                    [j.job_id for j in bqmonitor.jobs()],
                    self.service_name,
                )
                return self._request(
                    method=method,
                    endpoint=endpoint,
                    body=body,
                    query=query,
                    nojwt=nojwt,
                    jwtkvp=jwtkvp,
                    headers=headers,
                    stream=stream,
                    verbosity=verbosity,
                    follow_redirect=follow_redirect,
                )
            except ReplayError:
                raise

            return replay_response

        serializable_request = self.body_to_serializable_form(request)

        call = bqmonitor.Call(
            request=bqmonitor.Payload(body=serializable_request),
            service_name=self.service_name,
            replay_key=self.replay_key_for_body(request),
        )

        bqmonitor.publish(call)

        response = self._request(
            method=method,
            endpoint=endpoint,
            body=body,
            query=query,
            nojwt=nojwt,
            jwtkvp=jwtkvp,
            headers=headers,
            stream=stream,
            verbosity=verbosity,
            follow_redirect=follow_redirect,
        )
        body = self.body_to_serializable_form(response)
        call.response = bqmonitor.Payload(body=body)
        bqmonitor.publish(call)
        return response
