import uuid
from typing import Dict, Optional

from bqmonitor.json_serializable_type import JSONSerializable
from bqmonitor.payload import ErrorResponse, Payload
from bqmonitor.types import JSONSerializableType


class Call(JSONSerializable):
    """
    Description
    -----------
    An object storing information on a given request/response interaction with
    a service

    Attributes
    ----------
    request: Payload -
        the request sent to a given service

    response: Payload -
        the response from a given service for the request

    start_time: float -
        the time at which we issued the request to the outbound service

    end_time: float -
        the time at which we recieved the response from the outbound service
        for the given request

    duration: float -
        the time which it took to recieve the response for the given request

    Methods
    -------
    gt,lt,ge,le dunder methods -
        based on Call start time
    """

    def __init__(
        self,
        request: Payload,
        service_name: str,
        response: Optional[Payload] = None,
        error_response: Optional[ErrorResponse] = None,
        call_id: Optional[str] = None,
        stage: Optional[str] = None,
        job_id: Optional[str] = None,
        replay_key: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        request: Payload -
             the request sent to a given service

        response: Payload, optional -
            the response from a given service for the request

        service_name: str -
            the name of the service which this call is for
        """
        # it is assumed by design that you will not have a response and an
        # error response for a request
        assert not (error_response and response)
        self._request = request
        self._response = response
        self._error_response = error_response
        self._service_name = service_name
        self.stage = stage
        self.job_id = job_id
        self._call_id = call_id or uuid.uuid4().hex
        self.replay_key: Optional[str] = replay_key

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<service={self.service_name}, duration={self.duration}>"

    def __lt__(self, other: "Call") -> bool:
        return self.start_time < other.start_time

    def __gt__(self, other: "Call") -> bool:
        return self.start_time > other.start_time

    def __ge__(self, other: "Call") -> bool:
        return self.start_time >= other.start_time

    def __le__(self, other: "Call") -> bool:
        return self.start_time <= other.start_time

    def __hash__(self) -> int:
        return hash(self.call_id)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Call):
            raise TypeError
        return self.call_id == other.call_id

    @property
    def request(self) -> Payload:
        """
        Returns
        -------
        Payload - the request sent to a given service
        """
        return self._request

    @property
    def response(self) -> Optional[Payload]:
        """
        Returns
        -------
        Payload or None -
            the response from a given service for the request if the service
            has given a response, otherwise None
        """
        return self._response

    @response.setter
    def response(self, response: Payload) -> None:
        if not isinstance(response, Payload):
            raise TypeError(
                f"Expected response to be of type {Payload} but recieved type {type(response)}"
            )
        self._response = response

    @property
    def service_name(self) -> str:
        """
        Returns
        -------
        str - the name of the service to which this call reached out.
        """
        return self._service_name

    @property
    def start_time(self) -> float:
        """
        Returns
        -------
        float -
             the time at which we issued the request to the outbound service
        """
        return self.request.creation_time

    @property
    def end_time(self) -> Optional[float]:
        """
        Returns
        -------
        float or None -
            the time at which we recieved the response from the outbound service
            for the given request if the service has given a response, otherwise
            None
        """
        return self.response.creation_time if self.response else None

    @property
    def duration(self) -> float:
        """
        Returns
        -------
        float -
            the time which it took to recieve the response for the given request
            if the service has given a response, otherwise 0
        """
        if not self.end_time:
            return 0
        return self.end_time - self.start_time

    @property
    def call_id(self) -> str:
        """
        Returns
        -------
        str - unique identifier for this call.
        """
        return self._call_id

    def to_dict(self) -> Dict[str, JSONSerializableType]:
        """
        Description
        -----------
        Converts this call to a dict.

        Returns
        -------
        Dict[str, Any] - converted call.
        """
        return {
            "request": self.request.to_dict(),
            "service_name": self.service_name,
            "response": self.response.to_dict() if self.response else None,
            "stage": self.stage,
            "job_id": self.job_id,
            "call_id": self.call_id,
            "replay_key": self.replay_key,
        }

    @classmethod
    def from_dict(cls, dct: Dict[str, JSONSerializableType]) -> "Call":
        """
        Description
        -----------
        Convert dict to Call.

        Parameters
        ----------
        dct : Dict[str, Any] - dict to convert.

        Returns
        -------
        Call - converted call
        """

        return cls(
            request=Payload.from_dict(dct["request"]),
            service_name=dct["service_name"],
            response=Payload.from_dict(dct.get("response", {})),
            call_id=dct.get("call_id"),
            stage=dct["stage"],
            job_id=dct["job_id"],
            replay_key=dct["replay_key"],
        )
