# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from ..session import Session
from ..shim import shim_warning


@Session.register
@shim_warning
def list_fields(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.list_fields(self, *args, **kwargs)


@Session.register
@shim_warning
def get_field_info(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_field_info(self, *args, **kwargs)


@Session.register
@shim_warning
def get_overrides(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_overrides(self, *args, **kwargs)


@Session.register
@shim_warning
def find_fields(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.find_fields(self, *args, **kwargs)
