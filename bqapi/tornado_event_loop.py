# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import datetime


class TornadoEventLoop:

    """Wrap a tornado event loop suitable for use with `bqapi`.

    This class is a simple adaptor to make :class:`tornado.ioloop.IOLoop`
    conform to the interface expected by the :class:`Session` class or as
    an external event loop for the :class:`EventLoop` class.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~TornadoEventLoop.__init__
        ~TornadoEventLoop.call_later
        ~TornadoEventLoop.call_soon_threadsafe
        ~TornadoEventLoop.run_forever
        ~TornadoEventLoop.stop

    .. automethod:: TornadoEventLoop.__init__
    """

    class _Handle:

        def __init__(self, loop, handle):
            self._loop = loop
            self._handle = handle

        def cancel(self):
            self._loop.remove_timeout(self._handle)

    def __init__(self, tornado_loop=None):
        """Construct the adaptor given a tornado event loop.

        Parameters
        ----------
            tornado_loop : :class:`tornado.ioloop.IOLoop`, ``None``
                If `None`, a new :class:`tornado.ioloop.IOLoop` is constructed.
        """
        if tornado_loop is None:
            import tornado.ioloop
            self._loop = tornado.ioloop.IOLoop()
        else:
            self._loop = tornado_loop

    def run_forever(self):
        """Same as :meth:`tornado.ioloop.IOLoop.start()`."""
        self._loop.start()

    def stop(self):
        """Same as :meth:`tornado.ioloop.IOLoop.stop()`."""
        self._loop.stop()

    def call_later(self, delay, callback, *args):
        """Schedule a callback to be called after a certain delay.

        Same as :meth:`tornado.ioloop.IOLoop.call_later()`, but the returned
        handle has a `cancel()` method.
        """
        handle = self._loop.add_timeout(
            datetime.timedelta(seconds=delay), callback, *args)
        return TornadoEventLoop._Handle(self._loop, handle)

    def call_soon_threadsafe(self, callback, *args):
        """Same as :meth:`tornado.ioloop.IOLoop.add_callback()`."""
        return self._loop.add_callback(callback, *args)
