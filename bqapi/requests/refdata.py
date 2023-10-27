# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from ..session import Session
from ..shim import shim_warning


@Session.register
@shim_warning
def get_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_data(self, *args, **kwargs)


@Session.register
@shim_warning
def get_historical_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_historical_data(self, *args, **kwargs)


@Session.register
@shim_warning
def get_intraday_bar_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_intraday_bar_data(self, *args, **kwargs)


@Session.register
@shim_warning
def get_intraday_tick_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_intraday_tick_data(self, *args, **kwargs)


@Session.register
def get_eqs_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_eqs_data(self, *args, **kwargs)


@Session.register
def get_portfolio_data(self, *args, **kwargs):
    import pyrefdata
    return pyrefdata.get_portfolio_data(self, *args, **kwargs)
