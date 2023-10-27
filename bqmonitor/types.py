from datetime import datetime
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pandas import Timestamp

Date = Union[float, datetime, Timestamp, str]

_JSONSerializableLiteral = Union[str, int, float, bool, None]

JSONSerializableValue = Union[
    _JSONSerializableLiteral,
    Dict[_JSONSerializableLiteral, _JSONSerializableLiteral],
    List[_JSONSerializableLiteral],
]

JSONSerializableType = Union[
    JSONSerializableValue,
    Dict[str, JSONSerializableValue],
    List[JSONSerializableValue],
]

ExceptionInfo = Tuple[Type[BaseException], BaseException, TracebackType]
