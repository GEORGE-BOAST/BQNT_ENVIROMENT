from concurrent.futures import Future
from threading import Lock
import functools
import sys


class FutureExt(object):
    """
    This class provides a wrapper around a concurrent.futures.Future object. It
    delegates all of the latter's public functionality to the wrapped object,
    and in addition, provides helper functions when() and then() whose semantics
    are the same as in bqapi.Promise(). The reason we do this is to normalize
    the interface for some asynchronous operations done via bqapi and http.

    In the case of http, the asynchronisity is modelled using a
    ThreadPoolExecutor from concurrent.futures module and objects representing
    asyncronous execution are concurrent.futures.Future's. In the case of bqapi,
    these objects are bqapi.Promise's. With this wrapper we can write identical
    code for the two different execution paths (http and bqapi).

    Note: This module is not client facing and is used only in development when
    accessing bqgwsvc through http.
    """

    def __init__(self, future):
        self._future = future

    def cancel(self):
        return self._future.cancel()

    def cancelled(self):
        return self._future.cancelled()

    def running(self):
        return self._future.running()

    def done(self):
        return self._future.done()

    def result(self, timeout=None):
        return self._future.result(timeout)

    def exception(self, timeout=None):
        return self._future.exception(timeout)

    def add_done_callback(self, fn):
        return self._future.add_done_callback(fn)

    def set_running_or_notify_cancel(self):
        return self._future.set_running_or_notify_cancel()

    def set_result(self, result):
        return self._future.set_result(result)

    def set_exception(self, exc):
        return self._future.set_exception(exc)

    def then(self, callback, err_callback=None):
        out_future = FutureExt(Future())

        def on_done(future):
            assert self.done()

            try:
                future_result = self.result()
            except Exception as e1:

                if err_callback is None:
                    out_future.set_exception(e1)
                    return

                try:
                    err_callback_result = err_callback(sys.exc_info())
                except Exception as e2:
                    out_future.set_exception(e2)
                    return
                else:

                    # If the result of a handler is a FutureExt,
                    # we resolve it and propagate the result to the current
                    # future. This is done to be consistent with the
                    # behavior of bqapi.Promise.

                    if isinstance(err_callback_result, FutureExt):
                        err_callback_result.then(out_future.set_result)
                        return

                    out_future.set_result(err_callback_result)
                    return
            else:
                try:
                    cb_result = callback(future_result)
                except Exception as e3:
                    out_future.set_exception(e3)
                    return
                else:
                    # See comment above
                    if isinstance(cb_result, FutureExt):
                        cb_result.then(out_future.set_result)
                        return

                    out_future.set_result(cb_result)
                    return

        self.add_done_callback(on_done)
        return out_future

    @staticmethod
    def when(*futures):
        """
        Return a FutureExt that finishes when all futures are
        done
        """

        if not futures:
            raise ValueError("Empty futures.")

        out_future = FutureExt(Future())
        future_results = {}
        lock = Lock()

        def on_future_done(index, future):
            with lock:
                assert future.done()
                try:
                    future_results[index] = future.result()
                except Exception as e:
                    out_future.set_exception(e)
                    return

                if len(future_results) == len(futures):
                    out_future_result = [
                        result for _, result in sorted(future_results.items())
                    ]
                    out_future.set_result(out_future_result)

        for index, future in enumerate(futures):
            future.add_done_callback(functools.partial(on_future_done, index))

        return out_future
