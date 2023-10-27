# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import functools
import datetime
import logging

from six.moves import queue

from .promise import Promise
from .tornado_event_loop import TornadoEventLoop


class EventLoop:

    """Main event processing loop.

    The event processing loop is responsible for registering event handlers
    and executing them when the events they were registered for occurred.
    This typically includes asynchronous I/O events and timeout events.

    This event loop class is fairly minimal and provides all necessary
    features required by `bqapi`. It allows to schedule a callback to be
    called in the future (:meth:`call_later()`) or to schedule a callback
    from another thread to be called as soon as possible by the thread
    iterating the event loop (:meth:`call_soon_threadsafe()`).

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~EventLoop.__init__
        ~EventLoop.after
        ~EventLoop.call_soon_threadsafe
        ~EventLoop.call_later
        ~EventLoop.idle
        ~EventLoop.run_forever
        ~EventLoop.set_idle_call
        ~EventLoop.start
        ~EventLoop.start_reentrant
        ~EventLoop.stop
        ~EventLoop.stop_reentrant

    .. automethod:: EventLoop.__init__
    """

    class Timeout(BaseException):
        pass

    def __init__(self, queue_size=5, external_loop=None):
        """Construct a new event loop instance.

        This creates a new event loop object. It is typically not needed to
        create custom event loop, since most applications only need one event
        loop and they can use the one obtained with
        :meth:`bqapi.get_default_event_loop()` for that purpose.
        Constructing a custom event loop can be useful to adapt the
        `queue_size` parameter, or to integrate with an external event loop.

        The interface of this event loop is kept close to that
        of :class:`asyncio.BaseEventLoop`, however it is not a full replacement.
        It only implements the functionality needed by `bqapi`. However, this
        allows to plug a :class:`asyncio.BaseEventLoop` into the :class:`Session`
        class without any modifications, and :class:`tornado.ioloop.IOLoop` can
        be adapted easily. See :class:`~bqapi.TornadoEventLoop` for such an adaptor.

        Parameters
        ----------
            queue_size : int
                Specifies the maximum number of events in the event processing queue. If an
                event from a different thread is added to the main loop with
                :meth:`call_soon_threadsafe()`, it is added to a queue, and the event is
                removed from the queue when the corresponding event is being
                handled. When the queue is full, no further events can be added
                and :meth:`call_soon_threadsafe()` will block.
            external_loop : object, ``None``
                If not ``None``, this must be an instance with two methods called
                `call_soon_threadsafe()` and `call_later()`, such as
                :class:`asyncio.BaseEventLoop`. This allows to run the external
                event loop instead of this one, and whenever events occur, control
                will automatically be passed to this event loop, which processes
                the corresponding events, and then returns control to the external loop.
        """

        # TODO: Implement idle calls with timeouts; this simplifies the code and it
        # is already implemented anyway for external loops.

        # Limit the number of events in the queue. This avoids bufferbloat on
        # our side and enables better flow control if we are slow in keeping
        # up with processing the received data.
        self._queue = queue.Queue(queue_size)
        self._external_loop = external_loop
        self._timeouts = []
        self._looping = set()
        self._idle_call = None
        self._last_busy = None
        self._external_idle_handle = None

    def call_soon_threadsafe(self, callback):
        """Add a new event to the loop.

        The callback will be called when the main loop is iterated and the
        event being handled. This method should only be called from a thread
        that is *not* iterating the event loop, since it might block. This
        method blocks if the application is slow in processing the incoming
        events, to make the producer adapt to the speed at which events can
        be consumed.

        To schedule a callback to be called in a future event loop iteration
        from within the thread that runs the event loop, use
        :meth:`call_later()` with an interval of ``0``.

        The given callback should not raise any exceptions. If it does, the
        exception is catched and a warning message is printed to stderr upon
        execution of the event handler.

        Parameters
        ----------
            callback : callable
                The callback to call when the event is handled by the thread
                iterating the event loop. The callable is called with no
                arguments.
        """
        self._queue.put(callback)
        if self._external_loop:
            # TODO: If a spin is already scheduled externally, don't schedule another one...
            self._external_loop.call_soon_threadsafe(self._spin)

    def call_later(self, interval, callback):
        """Add an event which is called in `interval` seconds.

        Schedule `callback` to be called in `interval` seconds. This function
        must be called from the thread that is iterating the event loop. If
        you need to schedule a timeout from a different thread, combine
        :meth:`call_soon_threadsafe()` and :meth:`call_later()`.

        Parameters
        ----------
            callback : callable
                The callback to call after the timeout has elapsed. The
                callable is called with no arguments.
            interval : int, float
                Number of seconds until the timeout elapses.
        """

        class _RemoveHandle:

            def __init__(self, loop, timeout_obj):
                self.loop = loop
                self.timeout_obj = timeout_obj

            def cancel(self):
                self.loop._timeouts.remove(self.timeout_obj)

        # TODO: This should use monotonic time, not system time!!!
        # TODO: A min-heap would be an efficient data structure for this
        timeout_obj = (datetime.datetime.now() + datetime.timedelta(seconds=interval), callback)
        self._timeouts.append(timeout_obj)
        if self._external_loop:
            self._external_loop.call_later(interval, self._spin)

        return _RemoveHandle(self, timeout_obj)

    def start(self):
        """Start iterating the event loop.

        This function starts iterating the event loop with the caller's
        thread. Event handlers will be called as events occur. The function
        blocks until :meth:`stop()` is called by an event handler.

        This function is not re-entrant. For a re-entrant version see
        :meth:`start_reentrant()`.

        Note that when an external event loop has been set in the event loop
        constructor, this function is typically not called. Instead the external
        event loop's equivalent function should be used, and is scheduled as needed
        to process this loop's events. However, this function can be used to
        block until certain events occur, which is typically what happens when
        :meth:`Promise.result()` does when the promise is not yet done. However,
        in that case the external loop will not run while this call is in progress,
        and events sent only to the outer loop will not be processed. This can
        lead to a deadlock if the condition that would terminate this :meth:`start()`
        call depends on an event that is only received by the external event loop, and
        therefore such blocking calls when an external event loop is set up should
        be used with extreme caution.
        """
        return self.start_reentrant(None)

    def start_reentrant(self, iteration_id):
        """Re-entrant version of :meth:`start()`.

        This function is re-entrant if it is called with a different
        `iteration_id` than for the first call. The call to this
        function will then return when :meth:`stop_reentrant()` is called
        with the same iteration ID, and all subsequent re-entrant
        :meth:`start_reentrant()` calls have been
        :meth:`stop_reentrant()`-ed as well.

        An `iteration_id` with value ``None`` is typically reserved for the
        outermost call to this function. It is an error to call this function
        with an `iteration_id` that is already running.

        Parameters
        ----------
            iteration_id : hashable
                A hashable object which is used to match the :meth:`stop()` call
                in case the function is used re-entrantly.
        """

        if iteration_id in self._looping:
            raise ValueError(
                'The event loop is already iterating with the iteration ID "{}"'.format(iteration_id))

        self._looping.add(iteration_id)
        while iteration_id in self._looping:
            event = self._get_next_event()
            event()

    def stop(self):
        """Stop the event loop iteration.

        This function must only be called from an event handler that is called
        as a result of a previous call to :meth:`start()`. It will cause the
        event loop to terminate as soon as the current event handler returns,
        and make the :meth:`start()` call return. It is guaranteed that no
        further event handlers will be called after the current one has
        finished if no inner iterations of the loop are still running.
        """
        self.stop_reentrant(None)

    def stop_reentrant(self, iteration_id):
        """Stop an event loop iteration started with :meth:`start_reentrant()`.

        Causes the :meth:`start_reentrant()` call with the same `iteration_id` to
        return as soon as the control returns back to the event loop and no deeper-nested
        :meth:`start_reentrant()` calls are still running.

        Parameters
        ----------
            iteration_id : hashable
                A hashable object which is used to match the :meth:`start()` call
                that should return after this call.
        """
        if iteration_id not in self._looping:
            raise ValueError(
                'The event loop is not iterating with the iteration ID "{}"'.format(iteration_id))

        self._looping.remove(iteration_id)

    def run_forever(self):
        """Alias for :meth:`start()`.

        This function is provided as a synonym for :meth:`start()`. It is used
        by :meth:`Promise.result()` to wait until the promise finishes, so that
        :class:`asyncio.BaseEventLoop` can be used in place of this event loop
        class.
        """
        return self.start()

    def set_idle_call(self, callback=None, interval=None):
        """Set or disable an idle call after `interval` seconds.

        Schedule `callback` to be called continously when no other event
        has been received within `interval` seconds. If both `callback` and
        `interval` are ``None``, idle calls are disabled.

        If a callback has been previously set with this function, it will be
        deactivated. There can only be one idle callback active at a time.

        Parameters
        ----------
            callback : callable
                The callback to be called when no other event occurs
                within `interval` seconds. The callable is called with no
                arguments.
            interval : int, float
                The number of seconds with no activity before `callback`
                will be called.
        """

        # TODO: Allow multiple idle calls

        if self._external_idle_handle:
            self._external_idle_handle.cancel()
            self._external_idle_handle = None

        if (callback, interval) == (None, None):
            self._idle_call = None
            self._last_busy = None
        else:
            self._idle_call = (datetime.timedelta(seconds=interval), callback)
            self._last_busy = datetime.datetime.now()
            if self._external_loop:
                self._external_idle_handle = self._external_loop.call_later(
                    interval, self._on_external_idle)

    def idle(self, interval, result=None):
        """Return a promise which will repeatedly deliver when the event loop is idle.

        The returned :class:`Promise` instance is a multi-promise which can
        deliver values repeatedly instead of only once. The promise will deliver
        the value `result` every time the event loop has been idle for
        `interval` seconds.  The returned promise will never fail. This is
        a shallow wrapper around :meth:`set_idle_call()`.

        Parameters
        ----------
            interval : int, float
                The number of seconds with no activity before the promise
                delivers a value.
            result : object
                The value to deliver by the promies.
        """
        promise = Promise(self, True)
        self.set_idle_call(functools.partial(
            promise.set_result, result), interval)
        return promise

    def after(self, interval, result=None):
        """Return a promise which will deliver a fixed value in the future.

        The returned :class:`Promise` instance will deliver `result` in
        interval` seconds. This is a shallow wrapper around :meth:`call_later()`.
        The returned promise will never fail. This is a shallow wrapper around
        :meth:`call_later()`.

        Parameters
        ----------
            interval : int, float
                The number of seconds after which the returned promise should
                deliver a value.
            result : object
                The value to deliver by the promise.
        """
        promise = Promise(self)
        self.call_later(interval, functools.partial(
            promise.set_result, result))
        return promise

    def _spin(self, iteration_id=None):
        """Process all pending events.

        This function is similar to :meth:`start()`, in the sense that it starts
        the event loop and processes all incoming events. However, when no events
        are immediately available anymore, the function returns instead of blocking
        until more events are available. However, :meth:`stop()` can be called at
        any time by an event handler and it will cause this function to return
        immediately as well, even if more events would be available to be processed
        right away.

        Parameters
        ----------
            iteration_id : hashable
                A hashable object which is used to match the :meth:`stop()` call
                in case the function is used re-entrantly.
        """

        if iteration_id in self._looping:
            raise ValueError(
                'The event loop is already iterating with the iteration ID "{}"'.format(iteration_id))

        self._looping.add(iteration_id)
        while iteration_id in self._looping:
            try:
                event = self._get_next_event(0)
            except EventLoop.Timeout:
                self._looping.remove(iteration_id)
            else:
                event()

    def _get_next_event(self, timeout=None):
        """Returns the next event to be processed.

        An event is a callback function that has been passed to the event
        loop with :meth:`call_soon_threadsafe()`, :meth:`call_later()`, or :meth:`set_idle_call`. It is
        guaranteed that the returned callback will never raise an exception.

        This function will block until an event is available or the given timeout
        expires.

        Parameters
        ----------
            timeout : int, float, ``None``
                Maximum number of seconds to block. If this timeout expires with
                no event available, :class:`Timeout` is raised.
        """

        try:
            min_timeout = None

            # Determine next timeout to run, if any
            if self._timeouts:
                min_timeout = min(self._timeouts, key=lambda x: x[0])
                min_timeout_exp, min_timeout_cb = min_timeout

            # Overwrite with the idle handler, if any, and if it would run next
            if self._idle_call:
                idle_interval, idle_cb = self._idle_call
                idle_exp = self._last_busy + idle_interval
                if not self._timeouts or idle_exp < min_timeout_exp:
                    min_timeout = self._idle_call
                    min_timeout_exp, min_timeout_cb = idle_exp, idle_cb

            if min_timeout:
                now = datetime.datetime.now()

                # If the timeout already expired at the time that this function
                # is called, then return the corresponding event immediately.
                if now > min_timeout_exp:
                    if min_timeout != self._idle_call:
                        self._timeouts.remove(min_timeout)
                    return self._shield(min_timeout_cb)

                # Otherwise, determine the total time to wait
                wait_time = min_timeout_exp - now
                wait_time_secs = wait_time.total_seconds()
            else:
                wait_time_secs = None  # wait indefinitely

            if timeout is not None:
                if wait_time_secs is None or timeout < wait_time_secs:
                    wait_time_secs = timeout
                    min_timeout = None

            try:
                event = self._queue.get(True, wait_time_secs)
            except queue.Empty:
                # Timeout expired without having received an event
                if min_timeout is None:
                    raise EventLoop.Timeout
                elif min_timeout != self._idle_call:
                    self._timeouts.remove(min_timeout)
                return self._shield(min_timeout_cb)
            else:
                return self._shield(event)
        finally:
            # Schedule next idle call
            if self._idle_call:
                self._last_busy = datetime.datetime.now()
                if self._external_loop:
                    if self._external_idle_handle:
                        self._external_idle_handle.cancel()
                    self._external_idle_handle = self._external_loop.call_later(
                        self._idle_call[0].total_seconds(), self._on_external_idle)

    def _on_external_idle(self):
        self._external_idle_handle = None
        self._spin()

    def _shield(self, event):
        """Wraps a callback such that no exceptions propagate through the callback."""
        def shielded():
            try:
                event()
            except:
                logger = logging.getLogger('bqapi.EventLoop')
                logger.exception('Unhandled exception from event handler')

        return shielded


_default_loop = None


def _get_ipython_event_loop():
    """Return the event loop used by IPython.

    Return the event loop used when a ZMQ IPython kernel is used,
    or ``None`` if another kernel is used or we are not running
    within IPython.
    """
    # Define WindowsError if not defined (this will happen on Linux builds)
    try:
        WindowsError
    except NameError:
        WindowsError = ImportError

    try:
        import IPython
        import zmq

        ipython = IPython.get_ipython()
        if not ipython or not hasattr(ipython, 'kernel'):
            return None

        # xeus-python kernel does not have event loop
        if not hasattr(zmq, 'eventloop'):
            return None

        # Not needed, we are running a ZMQ kernel now...
        # kernel = ipython.kernel
        # if not isinstance(kernel, IPython.kernel.zmq.ipkernel.IPythonKernel):
        #    return None

        return zmq.eventloop.ioloop.IOLoop.instance()
    except WindowsError:
        # Happens when we are in a sandbox and not allowed to load the ZMQ
        # native DLL. Treat it like an import error; if ZMQ was supposed to
        # be allowed in the sandbox, it should have been imported before
        # lowering the token.
        return None
    except ImportError:
        return None


def get_default_event_loop():
    """Return the default event loop.

    The default event loop is a global event loop that is typically used
    by objects interacting with an event loop when no other event loop has
    been explicitly specified."""
    global _default_loop
    if not _default_loop:
        _ipython_loop = _get_ipython_event_loop()
        if _ipython_loop:
            _default_loop = EventLoop(
                external_loop=TornadoEventLoop(_ipython_loop))
        else:
            _default_loop = EventLoop()
    return _default_loop


def start():
    """Start the default event loop until :meth:`stop()` is called.

    This is a shortcut for::

        get_default_event_loop().start()
    """
    get_default_event_loop().start()


def stop():
    """Stop the default event loop started with :meth:`start()`.

    This is a shortcut for::

        get_default_event_loop().stop()
    """
    get_default_event_loop().stop()


def after(interval, result=None):
    """Return a promise which will deliver a fixed value in the future.

    This is a shortcut for::

        get_default_event_loop().after(interval, result)

    Parameters
    ----------
        interval : int, float
            The number of seconds after which the returned promise should
            deliver a value.
        result : object
            The value to deliver by the promise.
    """
    return get_default_event_loop().after(interval, result)


def idle(interval, result=None):
    """Return a promise which will repeatedly deliver when the event loop is idle.

    This is a shortcut for::

        get_default_event_loop().idle(interval, result)

    Parameters
    ----------
        interval : int, float
            The number of seconds with no activity before the promise
            delivers a value.
        result : object
            The value to deliver by the promise.
    """
    return get_default_event_loop().idle(interval, result)
