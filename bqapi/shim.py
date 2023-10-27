# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import warnings
from functools import wraps


WARNING_MESSAGE_TEMPLATE = (
    'Function {} has been moved to pyrefdata and will be removed soon from '
    'bqapi. Please port your code to use BQL or pyrefdata.'
)


def shim_warning(func):
    """Prints a warning message on function invocation"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(WARNING_MESSAGE_TEMPLATE.format(func.__name__),
                      UserWarning, stacklevel=2)
        return func(*args, **kwargs)
    return wrapper
