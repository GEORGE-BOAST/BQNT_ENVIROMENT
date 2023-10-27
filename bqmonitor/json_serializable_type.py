from abc import ABC, abstractclassmethod, abstractmethod
from typing import Any, Dict

from bqmonitor.types import JSONSerializableType


class JSONSerializable(ABC):
    @abstractmethod
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

    @abstractclassmethod
    def from_dict(cls, dct: Dict[str, JSONSerializableType]) -> Any:
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
