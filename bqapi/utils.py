# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import datetime
import collections

import dateutil
import dateutil.tz
import six


def scalar_or_list(x):
    if isinstance(x, (list, tuple)):
        for y in x:
            yield y
    elif x is not None:
        yield x


def dateify(value, default_date=None):
    if value is None:
        return default_date or datetime.date.today()
    elif isinstance(value, six.string_types):
        default_date = default_date or datetime.date.today()
        return dateutil.parser.parse(value, default=datetime.datetime(default_date.year,
                                                                      default_date.month,
                                                                      default_date.day)).date()
    elif isinstance(value, datetime.datetime):
        return value.date()
    elif isinstance(value, datetime.date):
        return value
    else:
        raise TypeError('Value "{}" not convertible into a date'.format(value))


def timeify(value, default_time=None):
    def now():
        return datetime.datetime.now(dateutil.tz.tzlocal()).timetz()

    if value is None:
        return default_time or now()
    elif isinstance(value, six.string_types):
        default_datetime = datetime.datetime.now()
        if default_time:
            default_datetime = default_datetime.replace(
                hour=default_time.hour, minute=default_time.minute,
                second=default_time.second, tzinfo=default_time.tzinfo)
        return dateutil.parser.parse(value, default=default_datetime).timetz()
    elif isinstance(value, datetime.datetime):
        return value.timetz()
    elif isinstance(value, datetime.time):
        return value
    else:
        raise TypeError('Value "{}" not convertible into a time'.format(value))


def datetimeify(value, default_datetime=None):
    def now():
        return datetime.datetime.now(tzinfo=dateutil.tz.tzlocal())

    if value is None:
        return default_datetime or now()
    elif isinstance(value, six.string_types):
        default_datetime = default_datetime or datetime.datetime.now()
        return dateutil.parser.parse(value, default=default_datetime)
    elif isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=default_datetime.tzinfo)
        return value
    elif isinstance(value, datetime.date):
        default_datetime = default_datetime or datetime.datetime.now()
        return datetime.datetime(value.year, value.month, value.day, default_datetime.hour, default_datetime.minute,
                                 default_datetime.second, default_datetime.microsecond, default_datetime.tzinfo)
    elif isinstance(value, datetime.time):
        default_datetime = default_datetime or datetime.datetime.now()
        return datetime.datetime(default_datetime.year, default_datetime.month, default_datetime.day,
                                 value.hour, value.minute, value.second, value.microsecond, value.tzinfo)
    else:
        raise TypeError(
            'Value "{}" not convertible into a datetime'.format(value))


def tz_from_name(tz_name, tzdf_tz):
    if isinstance(tz_name, datetime.tzinfo):
        return tz_name
    elif tz_name.lower() == 'local':
        return dateutil.tz.tzlocal()
    elif tz_name.lower() == 'utc':
        # Make sure we always return tzutc() for the UTC timezone, to avoid
        # this problem: https://github.com/dateutil/dateutil/issues/151
        return dateutil.tz.tzutc()
    elif tz_name.lower() == 'tzdf':
        if tzdf_tz is None:
            raise ValueError('TZDF cannot be resolved')
        return tzdf_tz
    else:
        tz = dateutil.tz.gettz(tz_name)
        if tz is None:
            raise ValueError('No such timezone "{}"'.format(tz_name))
        return tz


def _id(x):
    return x


class _Async(collections.abc.Mapping):

    def __init__(self, handler=None, error_handler=None):
        if handler is None:
            handler = _id

        self._handler = handler
        self._error_handler = error_handler

    def __len__(self):
        return 2

    def __iter__(self):
        return iter(['callback', 'error_callback'])

    def __getitem__(self, item):
        if item == 'callback':
            return self._handler
        elif item == 'error_callback':
            return self._error_handler
        raise KeyError(item)

    def __repr__(self):
        return repr(dict(self.items()))

    def __call__(self, handler=None, error_handler=None):
        return _Async(handler, error_handler)


asyn = _Async()
"""Signature for making service-interacting function calls asynchronous.

This attribute is a mapping which specifies the identity function as a
callback handler. Functions that query the Bloomberg
data service, such as :class:`Session.send_request()`, are typically blocking,
except a callback is given to them. Passing this signature to them makes
them asynchronous without specifying a callback, and the returned
:class:`Promise` instance will result in the return value of the call.
"""
