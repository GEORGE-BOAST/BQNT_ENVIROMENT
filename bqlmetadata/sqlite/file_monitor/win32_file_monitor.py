#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import ctypes
import threading

import six

from .file_monitor import FileMonitor


class Win32FileMonitor(FileMonitor):
    """Implement the FileMonitor interface using Win32 API.

    This implementation uses the FindFirstChangeNotification API.
    """
    def __init__(self):
        self._handle = None
        self._event = None
        self._thread = None
        self._notify_func = None

    def start(self, directory_name, notify_func):
        if self._handle is not None:
            raise RuntimeError('Already watching a directory')

        FILE_NOTIFY_CHANGE_FILE_NAME = 0x1
        FILE_NOTIFY_CHANGE_LAST_WRITE = 0x10

        if six.PY2:
            self._handle = ctypes.windll.kernel32.FindFirstChangeNotificationA(directory_name, False,
                                                                               FILE_NOTIFY_CHANGE_FILE_NAME |
                                                                               FILE_NOTIFY_CHANGE_LAST_WRITE)
        else:
            self._handle = ctypes.windll.kernel32.FindFirstChangeNotificationW(directory_name, False,
                                                                               FILE_NOTIFY_CHANGE_FILE_NAME |
                                                                               FILE_NOTIFY_CHANGE_LAST_WRITE)

        if self._handle == -1:
            self._handle = None
            raise WindowsError(ctypes.windll.kernel32.GetLastError())

        try:
            self._notify_func = notify_func

            self._event = ctypes.windll.kernel32.CreateEventA(0, True, False, 0)
            if not self._event:
                self._event = None
                raise WindowsError(ctypes.windll.kernel32.GetLastError())

            try:
                # Make the thread that reads change notifications a daemon thread,
                # so that if the python process exits without this object being
                # garbage-collected, we still exit the process.
                self._thread = threading.Thread(target=self._read_notifications)
                self._thread.daemon = True
                self._thread.start()
            except:
                ctypes.windll.kernel32.CloseHandle(self._event)
                self._event = None
                raise
        except:
            ctypes.windll.kernel32.FindCloseChangeNotification(self._handle)
            self._handle = None
            self._notify_func = None
            raise

    def stop(self):
        if self._handle is not None:
            # First, signal the thread to exit
            ctypes.windll.kernel32.SetEvent(self._event)

            # Then, wait for the thread to exit
            self._thread.join()

            # Then, clean up
            ctypes.windll.kernel32.CloseHandle(self._event)
            ctypes.windll.kernel32.FindCloseChangeNotification(self._handle)

            self._event = None
            self._handle = None
            self._thread = None
            self._notify_func = None

    def _read_notifications(self):
        handles = (ctypes.c_void_p * 2)(self._handle, self._event)
        while True:
            result = ctypes.windll.kernel32.WaitForMultipleObjects(2, handles, False, 0xffffffff)
            if result == 0:
                # a change notification occurred
                if not ctypes.windll.kernel32.FindNextChangeNotification(self._handle):
                    raise WindowsError(ctypes.windll.kernel32.GetLastError())
                self._notify_func()
            elif result == 1:
                # termination request was sent
                break
            elif result == -1 or result == 0xffffffff:
                # the WaitForMultipleObjects call failed.
                raise WindowsError(ctypes.windll.kernel32.GetLastError())
            else:
                raise RuntimeError(
                    f'Unexpected result from WaitForMultipleObjects():'
                    f' {result}')
