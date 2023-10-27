# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import blpapi
import threading
import functools


class EventThread(threading.Thread):

    def __init__(self, session, loop, handler):
        super(EventThread, self).__init__()
        self._session = session
        self._loop = loop
        self._handler = handler
        self.daemon = True

    def run(self):
        is_running = True
        while is_running:
            event = self._session.nextEvent()

            # Note that the lambda here does not work, otherwise, once the handler is invoked
            # from the main thread, the event than the handler is invoked with might change.
            # The functools.partial version binds the current value of the event variable into
            # the function call, and therefore always delivering the correct event.
            self._loop.call_soon_threadsafe(
                functools.partial(self._handler, event))

            # Stop the thread if the session has terminated
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                for msg in event:
                    if msg.messageType() in ['SessionStartupFailure', 'SessionTerminated']:
                        is_running = False
                        break
