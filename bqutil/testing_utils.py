from collections import defaultdict, namedtuple
from contextlib import contextmanager

import logging


_Log = namedtuple("_Log", ["records", "output"])


class CapturingHandler(logging.Handler):
    """Logging handler that captures messages."""

    def __init__(self):
        super(CapturingHandler, self).__init__()
        self.messages = defaultdict(list)
        self.logs = _Log([], [])

    def emit(self, record):
        self.messages[record.levelno].append(record.getMessage())
        self.logs.records.append(record)
        self.logs.output.append(':'.join([record.levelname,
                                          record.name,
                                          record.getMessage()]))


@contextmanager
def assertLogs(logger_name=logging.getLogger(), level=logging.INFO):
    """
    Context manager used to test logging functionality as described by
    pythons unittest's assertLogs:
    https://github.com/python/cpython/blob/032a6480e360427d4f964e31643604fad804ea14/Lib/unittest/case.py#L297

    An additional context manager is required here as unittest.TestCase.assertLogs
    is not available in pyhton 2 (New in version 3.4).
    """

    if isinstance(logger_name, logging.Logger):
        logger = logger_name
    else:
        logger = logging.getLogger(logger_name)

    handler = CapturingHandler()

    old_handlers = logger.handlers[:]
    old_level = logger.level
    old_propagate = logger.propagate

    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(level)

    yield handler.logs

    logger.handlers = old_handlers
    logger.propagate = old_propagate
    logger.setLevel(old_level)

    if len(handler.logs.output) == 0:
        raise AssertionError("No logs of level {} triggered on {}. Messages: {}"
                             .format(level, logger.name, handler.messages[level]))
