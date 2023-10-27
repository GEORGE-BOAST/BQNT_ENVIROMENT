# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.
from bql.common import client_context
import json
import copy

from .response_util import _check_response


class Job(object):
    """
    This object is returned when you submit a job to Async BQL.

    It can be used later to retrieve the response from the request.
    """

    __SUPPORTED_SCHEMA_MAJOR_VERSION = 1

    def __init__(self, response, execution_context=None):
        self._payload_id = response["payloadId"]
        execution_context = copy.deepcopy(execution_context)
        if execution_context is None:
            execution_context = {}
        execution_context["clientContext"] = response.get("clientContext", {})
        self._execution_context = execution_context

    @property
    def payload_id(self):
        return self._payload_id

    @staticmethod
    def get_data_item_indexes(execution_context):
        client_context = execution_context.get("clientContext")
        num_items = (
            len(execution_context["item_names"])
            if execution_context.get("item_names")
            else 1
        )
        # Ordering info may be encoded in client context's extra markers.
        ordering = None
        if client_context:
            for marker in client_context.get("extraMarkers", {}):
                if marker["key"] == "responsemetadata.source":
                    json_value = json.loads(marker["value"])
                    ordering = json_value["itemResponses"]
        # Ordering info trumps execution context because it comes from BQL
        return ordering or list(range(num_items))


    @property
    def execution_context(self):
        return self._execution_context

    @classmethod
    def from_submit_response(cls, response, execution_context):
        _check_response(
            response, Job.__SUPPORTED_SCHEMA_MAJOR_VERSION, execution_context
        )
        return cls(response, execution_context)
