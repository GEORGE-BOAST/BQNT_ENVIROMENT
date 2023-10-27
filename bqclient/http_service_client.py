import codecs
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from urllib.request import Request, urlopen

from .concurrent_futures_utils import FutureExt


class ServiceInfo(object):
    def __init__(self, service_name, major_version, minor_version):
        self.service_name = service_name
        self.major_version = major_version
        self.minor_version = minor_version

    def __str__(self):
        return "<ServiceInfo: {}:{}.{}>".format(
            self.service_name, self.major_version, self.minor_version
        )


class UserContext(object):
    def __init__(self, uuid=None, firm=None):
        self.uuid = uuid
        self.firm = firm

    def __str__(self):
        return "<UserContext: uuid={} firm={}>".format(self.uuid, self.firm)


# Engineering Test Account
# __DEFAULT_USER_CONTEXT = UserContext(uuid=20266934,
#                                      firm=135188)

# TODO: Determine best uuid to use
_DEFAULT_USER_CONTEXT = UserContext(uuid=6473368, firm=9001)


def _decode_response(stream):
    decoder = json.JSONDecoder()
    json_string = "".join(stream.read().split("\n"))

    offset = 0
    chunks = []

    while offset < len(json_string):
        (chunk, new_offset) = decoder.raw_decode(json_string, idx=offset)
        assert new_offset > offset

        chunks.append(chunk)
        offset = new_offset

    return chunks


class HttpServiceClient:
    def __init__(self, service_info, host, max_concurrent_requests=10):
        self._service_info = service_info
        self._host = host
        self._max_concurrent_requests = max_concurrent_requests
        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrent_requests
        )
        self._url = "http://{}:10799/{}".format(
            host, service_info.service_name
        )

    def _make_bas_request(self, request, user_context):

        assert user_context is not None

        if user_context.uuid <= 0:
            raise ValueError("uuid must be positive")

        service_version = "{}.{}".format(
            self._service_info.major_version, self._service_info.minor_version
        )
        user_info = [
            # This is the magic encoding for user identity.
            "/1.3.6.1.4.1.1814.3.1.1={}".format(user_context.uuid)
        ]
        firm = user_context.firm
        if firm:
            user_info.append("/1.3.6.1.4.1.1814.3.1.4={}".format(firm))

        return Request(
            self._url,
            data=json.dumps(request).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Connection": "close",
                "CCRT-Subject": ",".join(user_info),
                "com.bloomberg.bas-ServiceVersion": service_version,
            },
        )

    def _send_request_async(
        self, request, user_context, callback, error_callback, timeout
    ):

        assert callback is not None

        fut = self._executor.submit(
            self._send_request_sync, request, user_context, timeout
        )
        futex = FutureExt(fut)

        return futex.then(callback, error_callback)

    def _send_request_sync(self, request, user_context, timeout):

        assert user_context is not None  # checked in the caller

        bas_request = self._make_bas_request(request, user_context)
        reader = codecs.getreader("utf-8")
        stream = reader(urlopen(bas_request, timeout=timeout))

        with closing(stream) as f:
            return _decode_response(f)

    def send_request(
        self,
        request,
        user_context=_DEFAULT_USER_CONTEXT,
        callback=None,
        error_callback=None,
        timeout=None,
    ):

        if user_context is None:
            raise RuntimeError(
                "Sending http bas request requires UserContext with uuid"
            )

        is_sync = callback is None

        return (
            self._send_request_sync(request, user_context, timeout)
            if is_sync
            else self._send_request_async(
                request, user_context, callback, error_callback, timeout
            )
        )
