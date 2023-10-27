# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from abc import ABC

from ..common.string_request_executor import StringRequestExecutor
from ..om import Preferences


class RequestExecutor(ABC, StringRequestExecutor):

    def execute(self, request, preferences,
                callback=None, error_callback=None):
        """Executes the given BQL query request and return its response.

        `request` a :class:`Request` object.  Use by default `preferences`
        for the request execution, unless the `Request` overrides preferences
        on a setting-by-setting basis.  Returns a :class:`Response` instance
        or raises :class:`ResponseError` if the backend could not process
        the request.
        """

        request_string, item_names = self.__make_request_string(
            request, preferences)
        return self._execute_string(request_string,
                                    item_names,
                                    callback,
                                    error_callback)

    def submit(self,
               request,
               preferences,
               callback=None,
               error_callback=None):
        """Schedules the given BQL query request and return a payload id that
        the caller can use to retrieve the results when they become available.

        `request` a :class:`Request` object.  Use by default `preferences`
        for the request execution, unless the `Request` overrides preferences
        on a setting-by-setting basis.  Returns a :class:`Job` instance
        or raises :class:`ResponseError` if the backend could not process
        the request. If a callback is provided, then returns a Promise.
        """
        request_string, item_names = self.__make_request_string(
            request, preferences)
        return self._submit_string(request_string,
                                   item_names,
                                   callback,
                                   error_callback)

    def __make_request_string(self, request, preferences):
        """Make string_request and return items specified in OM."""
        request = self.__apply_prefs(request, preferences)
        request_string = request.to_string()
        item_names = [name for (name, _) in request.items.to_tuple_list()]
        return request_string, item_names

    @staticmethod
    def __apply_prefs(request, global_preferences):
        # Fold preferences into those already present in request if any.
        if global_preferences:
            # Start with the global preferences from the execute() call,
            # then override with any preferences specific to this request.
            new_preferences = global_preferences.to_dict()
            new_preferences.update(**request.preferences.to_dict())
            # Note that both the request preferences and global_preferences
            # have already been validated, and we're going to revalidate them
            # through the _with_preferences() call below.  We don't gain
            # anything by revalidating them, other than making the code
            # simpler.
            updated_request = request._with_preferences(
                Preferences(**new_preferences))
        else:
            updated_request = request
        return updated_request
