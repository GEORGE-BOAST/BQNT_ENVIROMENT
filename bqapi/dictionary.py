# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from .format import Format


class _DictionaryFormatFactory(Format):

    def __init__(self):
        self._handlers = {}

    def make_format(self, request_type, *args, **kwargs):
        """Create the format object."""
        if request_type not in self._handlers:
            raise Exception(
                'No dictionary output format available for request type "{}"'.format(request_type))
        return self._handlers[request_type](*args, **kwargs)

    def register_handler(self, *names):
        """Register a handler for a certain request type.

        This function is meant to be used as a class decorator to register a handler
        that turns a request response into a list of dictionaries. The decorated class
        should be a formatter instance as described in :ref:`output-formatters`.

        Parameters
        ----------
            names : list
                A list of strings, identifying the request types the decorated class is handling.
                This should be something that uniquely identifies the service and request,
                such as ``'CTK.GetSpotCurve'``, to avoid conflicts with other handlers.
                The only hard requirement, however, is that it matches the request type
                that is given to :meth:`Session.make_format` when the request is being sent.
        """
        def decorator(func):
            for name in names:
                if name in self._handlers:
                    raise AttributeError(
                        'Requests of type "{}" already have a handler!'.format(name))
                self._handlers[name] = func
            return func
        return decorator

    def __repr__(self):
        # Mostly to show something useful in the documentation
        return '{}()'.format(self.__class__.__name__)


dictionary = _DictionaryFormatFactory()
"""Return result as a list of python dictionaries.

This formatter produces a list of python dictionaries, where each item
in the list represents one row in the result dataset. Each dictionary entry
can be seen as one value for a particular result row.
"""
