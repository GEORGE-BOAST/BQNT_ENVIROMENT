import hashlib
import json
import os
from datetime import datetime
from typing import Dict, Union

from pandas import Timestamp

from .types import Date


def date_to_float(date: Date) -> float:
    """
    Description
    -----------
    For a given date returns that date as a float

    Parameters
    ----------
    date : Union[datetime, Timestamp, float] -
            the date to convert to a float

    Returns
    -------
    float -
            the converted date
    """
    if isinstance(date, datetime):
        return date.timestamp()
    if isinstance(date, Timestamp):
        return date.timestamp()
    if isinstance(date, str):
        return Timestamp(date).timestamp()
    try:
        return float(date)
    except ValueError:
        ValueError(f"Could not convert Date of type {type(date)} to float")


def read_json(file: Union[str, bytes]) -> Dict[str, object]:
    """
    Description
    -----------
    Read json file.

    Parameters
    -----------
    file : str -
        File to read.

    Returns
    --------
    Dict[str, object]
    """
    with open(file, "r") as io:
        return json.load(io)


def write_json(obj: Dict[str, object], file: Union[str, bytes]) -> None:
    """
    Description
    -----------
    Write obj to file as JSON while ensuring the file directory exists.

    Parameters
    -----------
    obj : Dict[str, object] -
        object to write to file.

    file : str -
        file to write to.

    Returns
    --------
    None
    """
    file_directory: str = os.path.dirname(file)
    mkdir_if_none(file_directory)
    with open(file, "w+") as io:
        json.dump(obj, io)


def mkdir_if_none(directory: str) -> None:
    """
    Description
    -----------
    Create a directory if it does not exist.

    Parameters
    -----------
    directory : str
        directory to check or create.

    Returns
    -------
    None
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
