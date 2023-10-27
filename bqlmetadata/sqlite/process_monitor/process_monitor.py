#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from abc import ABCMeta, abstractmethod
import six

"""
The interface declared in this module is a way for a process to notify
a dynamic number of "watcher" processes when a certain action has been
performed. This is used by the SQLite cache updater to ensure that there is
always only one process actively updating the cache, and all other processes
attempting to update the cache instead wait for the `active` cache to finish
its business.

The process who wants to notify other processes about the action should
instantiate and open a :class:`ProcessMonitorServer`, and store the return
value of that method together with a current timestamp at some well-known
location.

Processes who wish to be notified about the termination of the action being
performed by the active process should instantiate a
:class:`ProcessMonitorClient`, call the :meth:`ProcessMonitorClient.open`
method with the number and timestamp read from the location where the active
process has written it to. The :meth:`ProcessMonitorClient.wait` method can
then be used to wait for the active process to finish.
"""
@six.add_metaclass(ABCMeta)
class ProcessMonitorServer():
    """This class implements the functionality for the process to be monitored."""

    @abstractmethod
    def open(self):
        """Open the monitor server.

        Return an integer number that clients can use to monitor this process.
        """
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Close the monitor server.

        Notify monitor clients about termination of this process. If
        :meth:`open()` has not been called, this is a no-op.
        """
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

@six.add_metaclass(ABCMeta)
class ProcessMonitorClient():
    """This class implements the functionality to monitor a process."""
    
    @abstractmethod
    def open(self, pid, update_time):
        """Open a connection to a monitor server with the given number.

        `pid` is the number that the :meth:`ProcessMonitorServer.open()`
        method on the server returned. `update_time` is a
        :class:`datetime.datetime` instance which indicates a moment
        in time at which the process was known to be running.

        Returns ``False`` if there is no monitor server running with the given
        `pid`, or ``True`` if there is.
        """
        raise NotImplementedError()

    @abstractmethod
    def wait(self):
        """Wait for the monitor server to terminate.
        
        Before this method can be used, :meth:`open` must have been called and
        it must have returned `True`.
        """
        raise NotImplementedError()

    def close(self):
        """Close the connection to the monitor server.

        This must be called before this object is disposed if :meth:`open`
        returned with ``True`` before, to release any resources with the
        connection to the monitor server. If :meth:`open` was not called
        before, or if it returned ``False``, this is a no-op.
        """
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
