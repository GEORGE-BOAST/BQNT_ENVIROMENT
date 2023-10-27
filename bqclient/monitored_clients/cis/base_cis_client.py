import abc
from typing import Optional, Union

import jsonpickle

from bqclient.monitored_clients.cis.types import BaseURL
from bqmonitor.monitor_client_wrapper import MonitorClientWrapper
from bqmonitor.payload import ErrorResponse
from bqmonitor.types import JSONSerializableType


class BaseCISClient(MonitorClientWrapper, abc.ABC):
    def __init__(
        self,
        base_url: BaseURL,
        service_name: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        use_pets: bool = True,
    ):
        self._base_url = base_url
        self._service_name = service_name
        self._client_id = client_id
        self._client_secret = client_secret
        self._use_pets = use_pets
        if not use_pets:
            if client_id is None or client_secret is None:
                raise ValueError(
                    "Please provide `client_id` and `client_secret` if not using PETs authentication"
                )

    @property
    def service_name(self) -> str:
        return self._service_name

    def body_to_serializable_form(
        cls, body: object
    ) -> Union[JSONSerializableType, ErrorResponse]:
        return jsonpickle.encode(body)
