# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import
import six
import traceback

import sys
import functools
import logging
logger = logging.getLogger(__name__)


class _TbHolder:

    def __init__(self, exc_info):
        type, value, tb = exc_info
        self.type = type
        self.value = value

        # Format the exception already here to prevent the traceback
        # object having a reference to this object. This would make this
        # object being involved in a reference cycle, and, in Python 2.7,
        # objects in a reference cycle will never be garbage collected
        # if they have a __del__ method.
        # That's a bit unfortunate because we cannot give the original
        # exc_info to the logger. It can be fixed in python 3.4, where
        # the limitation of classes with a __del__ method not being
        # garbage collected has been lifted. In that case, we can
        # simply use Promise.__del__ for that case.
        self.formatted_tb = traceback.format_tb(tb)

    def clear(self):
        self.formatted_tb = None

    def __del__(self):
        if self.formatted_tb:
            logger = logging.getLogger('bqapi.Promise')
            logger.error('Exception set in promise was never consumed:\n%s%s',
                         ''.join(self.formatted_tb),
                         ''.join(traceback.format_exception_only(self.type, self.value)))


class Promise:

    """Placeholder object for asynchronous return values.

    A promise is a placeholder for a result from an asynchronous operation.
    It allows to wait for the result to be available either synchronously or
    asynchronously.

    A promise can be flagged as a multi-promise by setting the `multi` parameter to
    True in its constructor. While a normal promise typically produces a result and
    is immutable afterwards, a multi-promise can produce results repeatedly,
    representing recurring events or an asynchronous data stream.

    If the asynchronous operation fails, an exception object is stored with the promise
    instead of the result of the operation. Waiting for the promise synchronously will
    then re-raise the exception, and the exception is given to the asynchronous error
    handler, if any.

    Each promise is also associated to an event loop. The event loop is used to schedule
    calls for asynchronous handlers to make sure they are called with a
    clean context / call stack to avoid reentrancy issues. The promise should only
    be interacted with from the same thread that runs this event loop, and never from
    other threads. If another thread needs to set the result for a promise, it should
    schedule a call from the event thread using :meth:`EventLoop.call_soon_threadsafe()`.

    This implementation is fairly compatible with the A+ specification for promises
    in Javascript: https://github.com/promises-aplus/promises-spec.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~Promise.__init__
        ~Promise.__await__
        ~Promise.__iter__
        ~Promise.add_done_callback
        ~Promise.done
        ~Promise.future
        ~Promise.result
        ~Promise.set_exc_info
        ~Promise.set_result
        ~Promise.sync_then
        ~Promise.then
        ~Promise.when
        ~Promise.when_one

    .. automethod:: Promise.__init__
    """

    class Timeout(BaseException):
        """Raised by :meth:`~Promise.result` if the given timeout expires."""
        pass

    def __init__(self, event_loop, multi=False):
        """Construct a new promise object.

        The promise is initially pending. A result can be assigned with
        :meth:`set_result()` or :meth:`set_exc_info()`.

        Parameters
        ----------
        event_loop : :class:`EventLoop`
            An event loop that is used to schedule calls to :meth:`then()`
            functions from a clean context, and to iterate when
            :meth:`result()` is called.
        multi : bool
            If `True`, the promise is allowed to repeatedly deliver values
            by calling the :meth:`set_result()` or :meth:`set_exc_info()`
            methods multiple times.
        """
        self._event_loop = event_loop

        self._reply = None
        self._error = None
        self._finished = 0
        self._multi = multi
        self._exc_unnoticed = None

        self._then_handlers = []
        self._stop_func = None

    def done(self):
        """Return whether the asynchronous operation has completed.

        If this is returns True, and this is not a multi-promise, a call
        to :meth:`result()` is guaranteed not to block and return the result of
        the operation."""
        return self._finished > 0

    def set_result(self, result):
        """Provide a value for this promise.

        Once the asynchronous operation has completed, this function should
        be call to provide the result of the (successfully) completed operation.
        If the operation produced an error, use :meth:`set_exc_info()` instead.

        Parameters
        ----------
        result :
            The result of the asynchronous operation
        """
        self._resolve(result, None)

    def set_exc_info(self, exc_info):
        """Set an error for this promise.

        If the asynchronous operation has failed, the exception information should be
        set with this function.

        Parameters
        ----------
        exc_info : 3-tuple
            A ``(type, value, traceback)`` tuple of the exception that caused the
            operation to fail, as obtained with :meth:`sys.exc_info()`.
        """
        self._resolve(None, exc_info)

    def result(self, timeout=None):
        """Wait for the promise to finish and return its value.

        This function blocks the caller until one of :meth:`set_result()`
        or :meth:`set_exc_info()` is called. It iterates the event loop
        until one of the two happens.

        If :meth:`set_exc_info()` was called, the exception is re-raised.

        If the promise is not yet finished, this function is only allowed
        to be called from an event handler if the event loop supports
        re-entrant execution. The event loop provided by `bqapi`,
        :class:`bqapi.EventLoop`, supports this, however only with some
        limitations when it is used in combination with an external event
        loop. See :meth:`EventLoop.start()` for details. If the event loop
        does not support re-entrant execution, such as with :class:`TornadoEventLoop()`
        or :class:`asyncio.BaseEventLoop`, this method can only be called when the
        event loop is not already running, or the promise is guaranteed to be
        finished already. Otherwise, :meth:`add_done_callback()` should be
        used to be notified asynchronously when the result becomes available.

        Parameters
        ----------
        timeout : int, float, ``None``
            If not ``None``, this function will block at most this many seconds. If
            the result of the promise is not available after the time, :class:`~Promise.Timeout`
            is raised.
        """

        # If we already have a value, return or re-raise it
        if self._finished > 0 and not self._multi:
            if self._error:
                self._exc_noticed()
                six.reraise(self._error[0], self._error[1], self._error[2])
            return self._reply

        # We cannot wait for the result of this promise in two different main loop iterations
        # TODO: We could lift this restriction.
        assert self._stop_func is None

        # Figure out what functions to use to start and stop the event loop, depending
        # on whether it supports re-entrant iterations.
        if hasattr(self._event_loop, 'start_reentrant'):
            stop_func = functools.partial(
                self._event_loop.stop_reentrant, self)
            start_func = functools.partial(
                self._event_loop.start_reentrant, self)
        else:
            stop_func = self._event_loop.stop
            start_func = self._event_loop.run_forever

        # This is the stop function that we actually call when the promise has been resolved.
        # It stops the main loop and resets the stop function registered with this promise.
        def _local_stop_func():
            stop_func()
            self._stop_func = None

        self._stop_func = _local_stop_func
        prev_finished = self._finished
        timeout_handle = None

        # Set up the timeout
        if timeout is not None:
            timeout_handle = self._event_loop.call_later(
                timeout, _local_stop_func)

        # Run the event loop until the stop function is being called, either by the
        # timeout expiring or the promise being resolved.
        start_func()

        assert self._stop_func is None

        # If the promise has been resolved, reset the timeout and return the result,
        # otherwise raise Promise.Timeout.
        if self._finished > prev_finished:
            if timeout_handle is not None:
                timeout_handle.cancel()

            if self._error:
                self._exc_noticed()
                six.reraise(self._error[0], self._error[1], self._error[2])
            return self._reply
        else:
            assert timeout_handle is not None
            raise Promise.Timeout

    def then(self, handler=None, error_handler=None, remove=False):
        """Specify callbacks to run after the promise is finished.

        After completion of the promise, run `handler` with the result of the promise
        as the only argument, and return a new promise representing the result of the
        call to `handler`. If `handler()` returns another :class:`Promise` object, instead wait
        for that promise to finish, and return the value of that promise.

        If `error_handler` is not `None`, it is called if this promise fails with
        the exception info tuple as its only argument. The return value of the `error_handler`
        is set as the value of the returned promise with the same semantics as for the call
        to `handler`.

        If the call to `handler` or `error_handler` throws an exception, that exception will be
        set instead of the return value on the returned promise. If `handler`, `error_handler`, or
        both are `None`, then the result of this promise is directly propagated to the returned
        promise.

        If the promise is finished already, then the handlers are scheduled to be called as
        soon as possible, but not during the call to this function. Instead, they are called
        in a future event loop iteration.

        Parameters
        ----------
        handler : callable
            Function to be called when the promise finishes successfully.
        error_handler : callable
            Function to be called when the promise fails.
        remove : bool
            If ``True``, remove the handler after the promise has been resolved.
            If ``False``, keep the handler. If this is a multi-promise, it will be
            called again when the next value is delivered by the promise.
        """
        promise = Promise(self._event_loop, self._multi)
        then_handler = (handler, error_handler, promise, remove)

        if self._finished > 0:
            self._call_then_handler(then_handler)

        if self._finished == 0 or self._multi:
            self._then_handlers.append(then_handler)

        return promise

    def sync_then(self, handler=None, error_handler=None):
        """Register callbacks or run synchronously.

        This is a small helper function which is used to implement the
        "asynchronous-if-callback-or-error-callback-are-given"-behaviour
        of all service request functions.

        If either `handler` or `error_handler` are given, they are :meth:`then()`-ed
        to `promise` and the resulting promise is returned. Otherwise, if both are ``None``,
        the :meth:`result()` is returned, in which case this function blocks.
        """
        if handler or error_handler:
            return self.then(handler, error_handler)
        return self.result()

    def add_done_callback(self, fn):
        """Add a callback to be run when the promise finishes.

        The callback is called with a single argument - the promise object itself.
        This function is roughly equivalent to calling :meth:`then()`, with the same function as
        `handler` and `error_handler`, and is mostly provided for compatibility with Python 3's
        asyncio module.

        Parameters
        ----------
        fn : callable
            Function to be called with the promise as an argument once the
            promise either finishes or fails.
        """
        return self.then(lambda _: fn(self), lambda _: fn(self))

    @staticmethod
    def when(*promises):
        """Return a promise that finishes when all promises are done.

        The output of the returned promise is a list with the results
        of all promises. If any of the input promises fail, the output
        promise will fail immediately with the same error.
        """
        if not promises:
            raise ValueError('List of promises is empty')
        event_loops = set([promise._event_loop for promise in promises])
        if len(event_loops) > 1:
            raise ValueError('Promises operate with different event loops')

        n_promises = len(promises)
        out_promise = Promise(event_loops.pop())
        out_result = {}

        def on_done(index, result):
            out_result[index] = result
            if len(out_result) == n_promises and not out_promise.done():
                out_list = [res for _, res in sorted(out_result.items())]
                out_promise.set_result(out_list)

        def on_error(index, exc_info):
            if not out_promise.done():
                out_promise.set_exc_info(exc_info)

        for index, promise in enumerate(promises):
            handler = functools.partial(on_done, index)
            error_handler = functools.partial(on_error, index)
            promise.then(handler, error_handler)

        return out_promise

    @staticmethod
    def when_one(self, *promises):
        """Return a promise that finishes when one of the promises is done.

        The ouput of the returned promise will be a tuple with the promise
        that finished and its value. If any of the input promises fails, the
        output promise will fail immediately with the same error.
        """
        if not promises:
            raise ValueError('List of promises is empty')
        event_loops = set([promise._event_loop for promise in promises])
        if len(event_loops) > 1:
            raise ValueError('Promises operate with different event loops')

        out_promise = Promise(event_loops.pop())

        def on_done(promise, result):
            if not out_promise.done():
                out_promise.set_result((promise, result))

        def on_error(promise, exc_info):
            if not out_promise.done():
                out_promise.set_exc_info(exc_info)

        for promise in promises:
            handler = functools.partial(on_done, promise)
            error_handler = functools.partial(on_error, promise)
            promise.then(handler, error_handler)

        return out_promise

    def _resolve(self, result, error):
        assert self._finished == 0 or self._multi

        self._reply = result
        self._error = error
        self._finished += 1

        if error:
            self._exc_unnoticed = _TbHolder(error)
        else:
            self._exc_unnoticed = None

        # Stop blocking result() call if any
        if self._stop_func is not None:
            self._stop_func()

        # Release references to all callables. This is important because
        # they might in turn reference us, so we break reference cycles here
        # after the promise is resolved.
        then_handlers = self._then_handlers
        if not self._multi:
            self._then_handlers = None
        else:
            self._then_handlers = [(handler, error_handler, promise, remove)
                                   for handler, error_handler, promise, remove in self._then_handlers if not remove]

        # Call all then handlers, and deliver their promises.
        for then_handler in then_handlers:
            self._call_then_handler(then_handler)

    def _call_then_handler(self, then_handler):
        # Run the handler from the event loop so that every then handler
        # has a clean execution context.
        self._event_loop.call_later(0, functools.partial(
            self._exec_then_handler, then_handler))

    def _exec_then_handler(self, then_handler):
        assert self._finished > 0
        handler, error_handler, promise, remove = then_handler

        if self._error:
            self._exc_noticed()
            if error_handler:
                self._resolve_then_promise_from_handler(
                    promise, error_handler, self._error)
            else:
                promise.set_exc_info(self._error)
        else:
            if handler:
                self._resolve_then_promise_from_handler(
                    promise, handler, self._reply)
            else:
                promise.set_result(self._reply)

    def _resolve_then_promise_from_handler(self, promise, handler, argument):
        assert isinstance(promise, Promise)

        try:
            promise_result = handler(argument)
        except:
            exc_info = sys.exc_info()
            promise.set_exc_info(exc_info)
        else:
            # promise chaining if the result of the promise is thenable
            if hasattr(promise_result, 'then') and callable(promise_result.then):
                is_multi = hasattr(
                    promise_result, '_multi') and promise_result._multi
                # Propagate multi flag, so that when a then-handler for a non-multi promise
                # returns a multi promise, then promise returned by then() is a
                # multi promise.
                promise._multi = promise._multi or is_multi

                promise_result.then(promise.set_result, promise.set_exc_info)
            else:
                promise.set_result(promise_result)

    def _exc_noticed(self):
        if self._exc_unnoticed:
            self._exc_unnoticed.clear()
            self._exc_unnoticed = None

    def __iter__(self):
        """Return an iterator that yields from a future.

        This function returns a generator which yields an event-loop specific
        future. This is implemented for :class:`asyncio.BaseEventLoop` and
        :class:`tornado.ioloop.IOLoop`. If another event loop is used with the
        promise, :class:`NotImplementedError` is raised.

        This allows using promises with the coroutine functionality of asyncio
        or tornado, respectively. In other words, it allows to use the
        `yield from` or `await` keywords to wait until the result of the promise
        is available.
        """
        future = self.future()

        if hasattr(future, '__iter__') or hasattr(future, '__await__'):
            iterate_future = True
        else:
            iterate_future = False

        # This partly implements a "yield from" statement, without using "yield from" syntax,
        # to support python2 as well.
        return _PromiseGeneratorIter(future, iterate_future)

    __await__ = __iter__

    def future(self):
        """Return a future coupled to this promise.

        The future returned depends on the event loop associated with this promise.
        If a :class:`asyncio.BaseEventLoop` is used, a :class:`asyncio.Future` is
        returned. If a :class:`tornado.ioloop.IOLoop` is used, a
        :class:`tornado.concurrent.Future` is returned.

        For all other types of event loops, including :class:`bqapi.EventLoop`, the
        method raises :class:`NotImplementedError`.

        If this is a multi-promise, the returned future will be resolved for the next
        tick. This function can be called again in the handler of the future completion
        to obtain a new future for the subsequent tick.
        """
        from .event_loop import EventLoop
        loop = self._event_loop
        if isinstance(loop, EventLoop):
            if loop._external_loop is not None:
                loop = loop._external_loop

        future = None
        set_exc_info = None
        try:
            import asyncio
            if isinstance(loop, asyncio.BaseEventLoop):
                future = asyncio.Future(loop=loop)

                def set_exc_info(x):
                    return future.set_exception(x[1])
        except ImportError:
            asyncio = None

        if future is None:
            try:
                import tornado.ioloop
                from .tornado_event_loop import TornadoEventLoop
                if isinstance(loop, (tornado.ioloop.IOLoop, TornadoEventLoop)):
                    future = tornado.concurrent.Future()
                    set_exc_info = future.set_exc_info
            except ImportError:
                tornado = None
                TornadoEventLoop = None

        # We could check zmq ioloop as well, but zmq does not have futures, so
        # it's pointless.

        # If we don't have a future by this point, we don't know what kind of future
        # we need to generate.
        if future is None:
            raise NotImplementedError('Cannot create a future for event loop of type "{}"'.format(
                self._event_loop.__class__.__name__))

        if self.done() and not self.multi:
            if self._error is not None:
                set_exc_info(self._error)
            else:
                future.set_result(self.result())
        else:
            self.then(future.set_result, set_exc_info, remove=True)

        return future


class _PromiseGeneratorIter:
    def __init__(self, future, iterate_future):
        self._future = future
        self._iterate_future = iterate_future
        self._called = False
        # self._exc_info = None

    def next(self):
        if not self._called:
            self._called = True
            if self._iterate_future:
                # emulate "yield from" for asyncio: yield the first
                # (and only) element from the asyncio future
                return next(iter(self._future))
            else:
                # tornado needs to be yielded the future object itself
                return self._future
        else:
            # if self._exc_info is not None:
            #    six.reraise(self._exc_info[0], self._exc_info[1], self._exc_info[2])
            assert self._future.done()
            raise StopIteration(self._future.result())

    __next__ = next

    def send(self, x):
        # Asyncio uses send() instead of next(), but we don't need the value
        # we are sent.
        return self.next()

    # def throw(self, *x):
    #    #self._exc_info = x
    #    return self.__next__()
