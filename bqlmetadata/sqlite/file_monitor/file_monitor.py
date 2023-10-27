#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from abc import ABCMeta, abstractmethod
import six

@six.add_metaclass(ABCMeta)
class FileMonitor():
    @abstractmethod
    def start(self, directory_name, notify_func):
        """Start monitoring a given directory.

        Monitor the given directory for changes. When changes to any files
        within the directory occur, the given function is called on a
        secondary thread.

        A given :class`FileMonitor` instance can only monitor a single
        directory. Calling :meth:`start` more than once is a programming
        error.
        """
        raise NotImplementedError()

    def stop(self):
        """Stop watching the current directory.

        If the file monitor is not currently watching any directory, the
        function does nothing. After this function has returned,
        :meth:`start` can be called again with the same or a different
        directory to monitor.
        """
        raise NotImplementedError()
