# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

r"""

=====
PyBQL
=====

PyBQL is the Python Client API for the Bloomberg Query Language (BQL). The
API allows to send a request string to a BQL backend, and access the
result as a dataframe.
"""

# Make these available under the bql namespace:
from ._version import __version__
from .service import Service
from .om import (
    let,
    BqlItem,
    InvalidParameterGroupError,
    InvalidParameterNameError,
    InvalidParameterValueError,
    MissingParameterError,
    Preferences,
    UnexpectedParameterError,
    ValidationError
)
from .request import (
    combined_df,
    BqapiRequestExecutor,
    InvalidItemsError,
    InvalidUniverseError,
    Items,
    Request,
    RequestExecutor,
    Response,
    ResponseError,
    SingleItemResponse,
    UnexpectedResponseError,
    Universe
)
from bqlmetadata.literals import NA
