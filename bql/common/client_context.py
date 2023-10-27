# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import time
import uuid

_DEFAULT_APP_NAME = "BQNT"


class ClientContextBuilder(object):
    def __init__(self, app_name=_DEFAULT_APP_NAME, extra_markers=None):
        self.__context = {
            "appName": app_name,
            "clientRequestId": uuid.uuid4().hex,
            "timestamp": int(1000 * time.time())
        }
        if extra_markers is not None:
            self.add_extra_markers(extra_markers)

    def add_extra_markers(self, markers):
        context_markers = self.__context.setdefault("extraMarkers", [])
        for (key, val) in markers.items():
            context_markers.append({"key": str(key), "value": str(val)})

    def get_context(self):
        return self.__context
