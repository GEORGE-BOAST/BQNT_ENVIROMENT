from .blpapi_breg_service import BlpapiBregService
from .http_breg_service import HttpBregService

import logging
import time
from collections import namedtuple

_logger = logging.getLogger(__name__)

__breg_service = None


def __get_breg_service():
    global __breg_service

    if __breg_service is not None:
        return __breg_service

    try:
        import bqapi
    except ImportError:
        __breg_service = HttpBregService('nylxdev2')
    else:
        __breg_service = BlpapiBregService()

    return __breg_service


def install_breg_service(breg_service):
    global __breg_service
    global __cache
    __cache = {}
    __breg_service = breg_service


_CacheRecord = namedtuple('_CacheRecord', ['timestamp', 'value'])
__cache = {}

BQBREG_TYPE_INT = 0
BQBREG_TYPE_INTLIST = 1
BQBREG_TYPE_CHAR = 2
BQBREG_TYPE_STRING = 3
BQBREG_TYPE_DOUBLE = 4
BQBREG_TYPE_BOOLEAN = 5
BQBREG_TYPE_EXTINTLIST = 7
BQBREG_TYPE_EXTSTRING = 8


class BQBregEvalError(Exception):
    pass


def bqbreg_eval(accessor_name,
                entry_id,
                breg_type,
                default=None,
                expiry=0):
    """
    expiry: time in seconds before the cached value is invalidated.
    Use 0 to always go to the service and -1 to always use cached value
    """

    global __cache

    breg_service = __get_breg_service()

    now = time.time()

    try:
        cached_value = __cache.get(entry_id, None)

        if (cached_value is None or expiry == 0 or
                (now - cached_value.timestamp > expiry and expiry != -1)):
            bit_value = breg_service.evaluate_breg(accessor_name,
                                                   entry_id,
                                                   breg_type)
            __cache[entry_id] = _CacheRecord(timestamp=now, value=bit_value)
            return bit_value

        return cached_value.value
    except Exception as e:
        _logger.exception("Failed to get breg %d", entry_id)
        if default is not None:
            return default
        else:
            raise BQBregEvalError(
                "Failed to evaluate BREG {}".format(entry_id))


def bqbreg_eval_boolean(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id, BQBREG_TYPE_BOOLEAN,
                       default, expiry)


def bqbreg_eval_int(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id, BQBREG_TYPE_INT,
                       default, expiry)


def bqbreg_eval_intlist(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id, BQBREG_TYPE_INTLIST,
                       default, expiry)


def bqbreg_eval_char(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id, BQBREG_TYPE_CHAR,
                       default, expiry)


def bqbreg_eval_string(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id, BQBREG_TYPE_STRING,
                       default, expiry)


def bqbreg_eval_double(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id, BQBREG_TYPE_DOUBLE,
                       default, expiry)


def bqbreg_eval_extintlist(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id,
                       BQBREG_TYPE_EXTINTLIST,
                       default, expiry)


def bqbreg_eval_extstring(accessor_name, entry_id, default=None, expiry=0):
    return bqbreg_eval(accessor_name, entry_id,
                       BQBREG_TYPE_EXTSTRING,
                       default, expiry)
