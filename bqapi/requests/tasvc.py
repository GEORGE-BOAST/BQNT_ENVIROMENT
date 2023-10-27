# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from ..session import Session
from ..shim import shim_warning


@Session.register
@shim_warning
def get_study_list(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_study_list(self, *args, **kwargs)


@Session.register
@shim_warning
def get_study_attributes(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_study_attributes(self, *args, **kwargs)


@Session.register
@shim_warning
def get_historical_study_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_historical_study_data(self, *args, **kwargs)


@Session.register
@shim_warning
def get_intraday_bar_study_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_intraday_bar_study_data(self, *args, **kwargs)
