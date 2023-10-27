# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

r"""

=====
bqapi
=====

bqapi is a python API for the Bloomberg data service. It is built on top of
BLPAPI, and provides a more convenient and pythonic programming interface
while retaining most functionality offered by BLPAPI. It abstracts away
details such as event processing and correlation IDs.

Optionally, it provides data transformation features to further simplify
the interaction with the data service, for example by exposing requests
to well-known services as a convenient API or by returning requested data
in a :class:`pandas.DataFrame`.

Features
--------

Primary features of `bqapi` include:

    - Abstraction of low-level details such as event processing or keeping
      track of correlation IDs.
    - Supports synchronous and asynchronous retrieval of data.
    - Supports the publish/subscribe paradigm.
    - Provides data in different in various formats, such as Python
      dictionaries or :class:`pandas.DataFrame` instances.
    - Easily extensible by third parties for different output formats or
      Bloomberg services.
    - Interface to matplotlib and bqplot for data visualization.
    - Ideally suited for interactive data exploration in IPython notebooks.

Getting Started
---------------

The primary entry point to obtaining data from a Bloomberg service is the
:class:`bqapi.Session` class.

A simple data query with `bqapi` is as easy as this::

    import bqapi
    session = bqapi.Session()
    result = session.send_request('//blp/refdata', 'ReferenceDataRequest',
                                  {'securities': ['IBM US Equity'],
                                   'fields': ['PX_LAST']})

This queries the latest IBM price from the reference data service::

    >>> result
    [{'securityData': [{'eidData': [], 'security': 'IBM US Equity', 'fieldData': {'PX_LAST': 151.34}, 'sequenceNumber': 0L, 'fieldExceptions': []}]}] # noqa

Asynchronous Requests
---------------------

Making a request to the Bloomberg data service involves sending a request
over the network and waiting for a reply from the service. This is a
potentially lengthy operation, especially for large requests. Therefore,
`bqapi` offers the option to query data asynchronously::

    def result_available(data):
        return data

    promise = session.send_request('//blp/refdata', 'ReferenceDataRequest',
                                   {'securities': ['IBM US Equity'],
                                    'fields': ['PX_LAST']}, callback=result_available)

In this case, the function returns immediately and returns a :class:`Promise`,
which is basically a placeholder object for the return value of the given callback
function. There are two ways to deal with a promise. The
:meth:`Promise.result()` method blocks the execution until the result of
the promise is available, and returns the result::

    >>> promise.result()
    [{'securityData': [{'eidData': [], 'security': 'IBM US Equity', 'fieldData': {'PX_LAST': 151.34}, 'sequenceNumber': 0L, 'fieldExceptions': []}]}] # noqa

On the other hand, the :meth:`Promise.then()` method allows to schedule a function
to run once the result becomes available. This approach requires to run the
event processing loop to process events from the Bloomberg data service::

    def print_result(data):
        print data
        bqapi.stop()

    promise.then(print_result)
    bqapi.start()

The call to :meth:`bqapi.start()` starts the event processing loop. This is
a blocking call which returns as soon as :meth:`bqapi.stop()` is called.

The return value of :meth:`Promise.then()` is another promise, which finishes
with the return value of the function that is executed by :meth:`Promise.then()`.
This allows to easily chain asynchronous calls.

Other than `callback`, there is also an `error_callback` parameter, which allows
to schedule a callable to be called with a ``(type, value, traceback)``-triplet
when the asynchronous operation fails.

Real-Time Subscriptions
-----------------------

Other than one-time data requests, `bqapi` also supports permanent
subscriptions to services. This allows monitoring market data in real-time.
The :meth:`~Session.send_subscribe()` function is used to make a market data
subscription. It returns a :class:`Subscription` object::

    subscription = session.send_subscribe('//blp/mktdata', 'ticker/IBM US Equity', 'LAST_PRICE')

The returned subscription instance can be used for three purposes: To
receive market data updates, to be notified when the subscription is cancelled
(for example when the server goes down), or to unsubscribe from the service.

To receive market data updates, the function :meth:`Subscription.on_update()`
returns a special kind of promise which results in the next tick from the
market data service. It is special in the sense that it can produce results
repeatedly and not only once. For example, to print all market data ticks
as they happen::

    def print_update(data):
        print data

    subscription.on_update().then(print_update)
    bqapi.start()

To be notified when the subscription is established or cancelled, the
:meth:`Subscription.on_subscribe()` and :meth:`Subscription.on_unsubscribe()` functions can be
used, respectively. They return a promise which results in the :class:`Subscription` object itself
when the subscription is started or stopped. To actively unsubscribe from the service,
:meth:`Session.unsubscribe()` can be called with the :class:`Subscription` instance as an argument::

    session.unsubscribe(subscription)

Different Output Data Structures
--------------------------------

By default, `bqapi` returns the raw BLPAPI responses wrapped in :class:`ListElement`
and :class:`DictElement` instances that allow to iterate through the response
as if they were python lists or dictionaries. However,
the data can also be returned in different data structures by providing the `format`
parameter to :meth:`Session.send_request` or :meth:`Session.send_subscribe`. See
:ref:`output-formatters` for details.

API Reference
-------------

The following is a reference of the full `bqapi` API.

.. currentmodule:: bqapi

**Classes**

.. autosummary::
    :nosignatures:
    :toctree: generate/
    :template: bpclass.tpl

    Session
    Settings
    Subscription
    Promise
    EventLoop
    TornadoEventLoop
    MessageElement
    DictElement
    ListElement
    Format
    DictPlotAdaptor
    MPLPlotter
    BQPlotter

.. toctree::
    exceptions

**Module Functions and Attributes**

.. autosummary::
    :nosignatures:
    :toctree: generate/

    get_default_event_loop
    make_format
    start
    stop
    after
    idle
    asyn

**Output Formatters**

.. toctree::
    :hidden:

    formatters

See :ref:`output-formatters` for what a formatter is and how to create your own.

.. autosummary::
    :nosignatures:
    :toctree: generate/

    no_format
    dictionary
    data_frame
    plot
    mplplot
    bqplot

**Extensions**

.. toctree::
    :maxdepth: 1

    event_loop_integration
    coroutines
    services
"""

from __future__ import absolute_import

from ._version import __version__

from .session import Session
from .session_singleton import get_session_singleton
from .settings import Settings
from .event_loop import EventLoop, start, stop, after, idle, get_default_event_loop
from .tornado_event_loop import TornadoEventLoop
from .error import (
    SessionClosedError,
    AuthorizationError,
    ServiceClosedError,
    SubscriptionClosedError,
    RequestError,
    SecurityError,
    FieldError,
    StudyError,
    RequestTimedOutError)
from .format import Format, make_format, no_format
from .dictionary import dictionary
from .data_frame import data_frame
from .plot import DictPlotAdaptor, plot
from .mplplot import MPLPlotter, mplplot
from .bqplot import BQPlotter, bqplot
from .promise import Promise
from .request import Subscription
from .element import MessageElement, DictElement, ListElement, wrap
from .utils import asyn, scalar_or_list, tz_from_name

from .requests import *
