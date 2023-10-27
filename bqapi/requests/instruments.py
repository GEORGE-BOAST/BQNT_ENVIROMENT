# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from ..session import Session
from ..shim import shim_warning


@Session.register
@shim_warning
def find_securities(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.find_securities(self, *args, **kwargs)


@Session.register
@shim_warning
def find_curves(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.find_curves(self, *args, **kwargs)


@Session.register
@shim_warning
def find_govt(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.find_govt(self, *args, **kwargs)
