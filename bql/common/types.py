# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import ciso8601
import numpy as np
import pandas

from .response_error import UnexpectedResponseError

_PD_TYPES = {
    'INT': 'int64',
    'BOOLEAN': 'bool',
    'DOUBLE': 'float64',
    'STRING': 'object',
    'ENUM': 'object',
    'SYMBOL': 'object',
    'DATE':  'datetime64[ns, UTC]',
    'DATETIME': 'datetime64[ns, UTC]'
}

_NP_TYPES = {
    'INT': 'int64',
    'BOOLEAN': 'bool',
    'DOUBLE': 'float64',
    'STRING': 'object',
    'ENUM': 'object',
    'SYMBOL': 'object',
    'DATE':  'datetime64[ns]',
    'DATETIME': 'datetime64[ns]'
}


def _int_to_py_type(bql_value):
    if bql_value is None:
        # TODO: can this happen?
        return 0
    return int(bql_value)

def _bool_to_py_type(bql_value):
    if bql_value is None:
        return False

    return bool(bql_value)

def _double_to_py_type(bql_value):
    if bql_value is None:
        return float('nan')
    else:
        return float(bql_value)

def _string_to_py_type(bql_value):
    if bql_value is None:
        return None
    return str(bql_value)

def _date_to_py_type(bql_value):
    # bql_value is of the form '%Y-%m-%dT%H:%M:%SZ'
    if bql_value is None:
        return None

    # We omit the last 'Z' character to avoid numpy exception
    return np.datetime64(bql_value[:-1])


def _datetime_to_py_type(bql_value):
    if bql_value is None:
        return None
    # ciso8601 is faster than strptime.
    # See https://bbgithub.dev.bloomberg.com/bqnt/pybql/pull/368
    return ciso8601.parse_datetime(bql_value)


def _convert_to_py_list(bql_values, bql_type, tz_aware=False):
    conversions = {
        'INT': _int_to_py_type,
        'BOOLEAN': _bool_to_py_type,
        'DOUBLE': _double_to_py_type,
        'STRING': _string_to_py_type,
        'ENUM': _string_to_py_type,
        'DATE': _datetime_to_py_type if tz_aware else _date_to_py_type,
        'DATETIME': _datetime_to_py_type,
        'SYMBOL': _string_to_py_type
    }

    converter = conversions.get(bql_type)
    if converter is None:
        raise UnexpectedResponseError(f'No converter for BQL type "{bql_type}"')
    return [converter(value) for value in bql_values]

def convert_to_pd_series(bql_values, bql_type, series_name):
    """
    Converts an iterable of values to a pandas series. Uses timezone aware
    dates.
    """
    pd_type = _PD_TYPES.get(bql_type)
    if pd_type is None:
        raise UnexpectedResponseError(
            f'No pandas type for BQL type "{bql_type}"')

    py_values = _convert_to_py_list(bql_values, bql_type, tz_aware=True)
    series = pandas.Series(py_values, dtype=pd_type, name=series_name)
    return series

def convert_to_np_array(bql_values, bql_type):

    py_values = _convert_to_py_list(bql_values, bql_type)

    np_type = _NP_TYPES.get(bql_type)
    if np_type is None:
        raise UnexpectedResponseError(
            f'No numpy type for BQL type "{bql_type}"')

    return np.array(py_values, dtype=np_type)
