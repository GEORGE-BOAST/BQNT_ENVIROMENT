import time
import uuid
from typing import Any, Dict, List, Optional, Union

import jsonpickle
from typing_extensions import Type

from bqmonitor.json_serializable_type import JSONSerializable
from bqmonitor.types import JSONSerializableType, JSONSerializableValue


class ErrorResponse(JSONSerializable):
    """
    Description
    -----------
    An object representing a serializable form of an Error

    Attributes
    -----------
    body: dict -
        the body of the request or response to or from the given service

    creation_time: float -
        the time at which this Payload object was created
    """

    def __init__(
        self,
        error_type: str,
        error_message: str,
    ):
        """
        Parameters
        ----------
        body: dict, optional(default = None) -
            the body of the request or response to or from the given service

        creation_time: float, optional(default = None) -
            time payload was created
        """
        self._error_type: str = error_type
        self._error_message: str = error_message

    @property
    def error_type(self) -> str:
        return self._error_type

    @property
    def error_message(self) -> str:
        return self._error_message

    def __eq__(self, other: "ErrorResponse") -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError
        return (
            self.error_type == other.error_type
            and self.error_message == other.error_message
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<Error={self._error_type}({self._error_message})>"

    def to_dict(self) -> Dict[str, JSONSerializableType]:
        """
        Description
        -----------
        Convert ErrorResponse to dict.

        Returns
        -------
        Dict[str, Any] -
            converted Payload.
        """
        return {
            "error_type": self._error_type,
            "error_message": self._error_message,
        }

    def as_exception(self) -> BaseException:
        def find_exception_for_name(
            base: Type[BaseException],
        ) -> Optional[BaseException]:
            if base.__name__ == self._error_type:
                return base(self._error_message)
            for child_class in base.__subclasses__():
                result = find_exception_for_name(child_class)
                if result:
                    return result

        built_in_exception = find_exception_for_name(BaseException)
        if built_in_exception:
            return built_in_exception
        else:
            custom_exception_class: Type[Exception] = type(
                self._error_type, (Exception,), {}
            )
            return custom_exception_class(self._error_message)

    @classmethod
    def from_exception(cls, exception: BaseException) -> "ErrorResponse":
        return cls(exception.__class__.__name__, str(exception))

    @classmethod
    def from_dict(
        cls, dct: Dict[str, JSONSerializableType]
    ) -> "ErrorResponse":
        """
        Description
        -----------
        Convert dict to ErrorResponse.

        Parameters
        ----------
        dct : Dict[str, Any] -
            dict to convert.

        Returns
        -------
        Payload : -
            converted payload.
        """
        return cls(
            error_type=dct.get("error_type", ""),
            error_message=dct.get("error_message", ""),
        )


class Payload(JSONSerializable):
    """
    Description
    -----------
    An object storing the data of a request or response to or from a service

    Attributes
    -----------
    body: dict -
        the body of the request or response to or from the given service

    creation_time: float -
        the time at which this Payload object was created
    """

    def __init__(
        self,
        body: Union[JSONSerializableValue, ErrorResponse],
        creation_time: Optional[float] = None,
    ):
        """
        Parameters
        ----------
        body: dict, optional(default = None) -
            the body of the request or response to or from the given service

        creation_time: float, optional(default = None) -
            time payload was created
        """
        self._creation_time: float = creation_time or time.time()
        self._body: Union[JSONSerializableValue, ErrorResponse] = body

    @property
    def body(self) -> Union[JSONSerializableValue, ErrorResponse]:
        """
        Returns
        -------
        dict or None -
            the body of the object if given otherwise None
        """
        try:
            return jsonpickle.decode(self._body)
        except Exception as e:
            return self._body

    @property
    def creation_time(self) -> float:
        """
        Returns
        -------
        float -
            the time at which this Payload object was created
        """
        return self._creation_time

    def __eq__(self, other: "Payload") -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError
        return (
            self.body == other.body
            and self.creation_time == other.creation_time
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<body={str(self._body)}>"

    def to_dict(self) -> Dict[str, JSONSerializableType]:
        """
        Description
        -----------
        Convert Payload to dict.

        Returns
        -------
        Dict[str, Any] -
            converted Payload.
        """
        return {
            "body": self._body
            if not isinstance(self.body, JSONSerializable)
            else self.body.to_dict(),
            "creation_time": self.creation_time,
        }

    @classmethod
    def from_dict(
        cls, dct: Optional[Dict[str, JSONSerializableType]]
    ) -> "Payload":
        """
        Description
        -----------
        Convert dict to Payload.

        Parameters
        ----------
        dct : Dict[str, Any] -
            dict to convert.

        Returns
        -------
        Payload : -
            converted payload.
        """
        dct = dct or {}
        creation_time = dct.get("creation_time")
        return cls(
            body=dct.get("body", {}),
            creation_time=float(creation_time) if creation_time else None,
        )
