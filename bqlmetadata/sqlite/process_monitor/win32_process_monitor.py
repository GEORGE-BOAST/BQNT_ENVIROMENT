#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import os
import ctypes

from .process_monitor import ProcessMonitorServer, ProcessMonitorClient


def _get_process_creation_time(proc):
    """Obtain the time the given process was created.
    
    `proc` must be a valid HANDLE for a process. The function returns a unix
    timestamp. This function is only available on Windows.
    """
    class FILETIME(ctypes.Structure):
        _fields_ = [("dwLowDateTime", ctypes.c_uint32),
                    ("dwHighDateTime", ctypes.c_uint32)]

    creation = FILETIME()
    exit = FILETIME()
    kernel = FILETIME()
    user = FILETIME()

    retval = ctypes.windll.kernel32.GetProcessTimes(proc,
                                                    ctypes.byref(creation),
                                                    ctypes.byref(exit),
                                                    ctypes.byref(kernel),
                                                    ctypes.byref(user))

    if retval != 1:
        raise WindowsError(ctypes.windll.kernel32.GetLastError())

    # Convert from Windows FILETIME to a unix timestamp
    windows_ticks = 10000000
    offset = 11644473600
    filetime = creation.dwHighDateTime << 32 | creation.dwLowDateTime
    return filetime / windows_ticks - offset

def _open_pid(pid, update_time):
    """Given a PID, open a handle to the process with that PID.

    Return the handle if the process exists or ``None`` if it does not.
    `update_time` is a unix timestamp indicating a point in time at which the
    process was known to be running. If we find a process with the same PID
    that was started after the given point in time, then we assume it is a
    different process, i.e. the original process terminated and its PID
    got re-assigned.
    """

    # Open a handle to the process
    proc = ctypes.windll.kernel32.OpenProcess(2035711, 0, pid)

    # If the process does not exist, we get a NULL handle
    result = None
    if proc != 0:
        try:
            # Note that there are typically no race conditions here since 
            # we have a lock on the database, and the process cannot
            # terminate before it gets a lock.
            exit_code = ctypes.c_uint32(0)
            if not ctypes.windll.kernel32.GetExitCodeProcess(proc, ctypes.byref(exit_code)):
                raise WindowsError(ctypes.windll.kernel32.GetLastError())

            if exit_code.value == 259: # 259 is returned if the process is still active
                # Now, check the process creation time. Note this trick only
                # works as long as the clock of the computer does not make a
                # jump.
                creation_time = _get_process_creation_time(proc)
                if creation_time <= update_time:
                    result = proc

        finally:
            if result is None:
                ctypes.windll.kernel32.CloseHandle(proc)
        return result

def _wait_pid(proc):
    """Wait for the given process to terminate."""
    handle, exit_code = os.waitpid(proc, 0)

def _close_pid(proc):
    """Close the given process HANDLE."""
    ctypes.windll.kernel32.CloseHandle(proc)

class Win32ProcessMonitorServer(ProcessMonitorServer):
    """Implement the process monitor interface using Win32 process HANDLEs.

    This method only works if the process actually terminates right after
    finished its "active" duty.
    """
    def __init__(self):
        self._is_open = False

    def open(self):
        self._is_open = True
        return os.getpid()

    def close(self):
        #if self._is_open:
        #    sys.exit(0)
        pass

class Win32ProcMonitorClient:
    """Implement the process monitor interface using Win32 process HANDLEs.

    This method only works if the process actually terminates right after
    finished its "active" duty.
    """
    def __init__(self):
        self._proc = None

    def open(self, pid, update_time):
        self._proc = _open_pid(pid, update_time)
        return self._proc is not None

    def wait(self):
        _wait_pid(self._proc)

    def close(self):
        if self._proc is not None:
            _close_pid(self._proc)
