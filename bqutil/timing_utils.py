import time
from functools import wraps

import logging

_logger = logging.getLogger(__name__)


class ScopedTracer(object):
    """
    A context manager that allows for logging the timing of the
    execution of the enclosed block. Also allows publishing events
    and their durations to the backend through bqevents module.
    """

    def __init__(self,
                 name,
                 logger=None,
                 level=logging.INFO,
                 publish=False,
                 **kwargs):
        """
        Parameters
        ----------

        name: str
            The name of the logged event, or in the case when publish is True,
            the event type (unless event_type is overridden in kwargs)
        logger: logging.Logger
            The logger which will be used to log the event
        level: logging level
        publish: bool
            Indicates whether to publish the event and its duration
            to the backend through bqevents
        **kwargs:
            If publish is True, this is a dictionary that will be published
            to bqevents as part of the payload.
        """

        self._name = name
        self._start_time = None
        self._end_time = None
        self._logger = logger or _logger
        self._level = level
        self._publish = publish
        self._bqevents_args = kwargs

    @property
    def elapsed_time(self):
        return (time.perf_counter() - self._start_time if self._end_time is None
                else self._end_time - self._start_time)

    def __enter__(self):
        args = self._bqevents_args
        if args and self._logger.isEnabledFor(self._level):
            arg_str = ' ({})'.format(','.join('{}={!r}'.format(k, args[k])
                                              for k in sorted(args)))
        else:
            arg_str = ''

        self._logger.log(self._level, "Start timer on event: %s%s", self._name,
                         arg_str, extra={'publish': False})
        self._start_time = time.perf_counter()
        return self

    def __exit__(self, type_, value, traceback):
        self._end_time = time.perf_counter()

        elapsed = self.elapsed_time
        payload = {'event_type': self._name, 'duration': elapsed}
        payload.update(self._bqevents_args)
        extra = {'publish': self._publish, 'payload': payload}

        if type_ is None:
            self._logger.log(self._level,
                             "Event %s completed after %f seconds",
                             self._name, elapsed, extra=extra)
        else:
            self._logger.log(self._level,
                             "Event %s completed after %f seconds"
                             " Exception thrown %s: %s",
                             self._name, elapsed, type_, value, extra=extra)


def trace_it(logger=None,
             level=logging.INFO,
             publish=False,
             name=None,
             log_args_level=logging.NOTSET,
             **decorator_kwargs):
    """
    A decorator that allows to emit log traces containing the timing
    of the execution of the decorated function. If publish is True,
    the decorator_kwargs will be published as payload to bqevents
    """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            logger_ = logger or _logger
            name_ = name or func.__name__

            if log_args_level != logging.NOTSET:
                logger_.log(
                    log_args_level,
                    "Executing %s with args=%s, kwargs=%s",
                    name_, args, kwargs)

            with ScopedTracer(name=name_,
                              logger=logger_,
                              level=level,
                              publish=publish,
                              **decorator_kwargs):
                return func(*args, **kwargs)

        return wrapper

    return decorator
