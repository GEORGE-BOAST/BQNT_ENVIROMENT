# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import collections
import logging
logger = logging.getLogger(__name__)


class Format(collections.abc.Mapping):

    """Base class for output formatters. See :ref:`output-formatters`.

    Deriving from this class allows the formatter to be passed
    as ``**formatter`` in addition to ``format=formatter``.
    """

    def __len__(self):
        return 1

    def __iter__(self):
        return iter(['format'])

    def __getitem__(self, item):
        if item == 'format':
            return self
        raise KeyError(item)

    def __repr__(self):
        # Mostly to show something useful in the documentation
        return '{}()'.format(self.__class__.__name__)


class SimpleFormat(Format):
    """Simple formatter without any additional processing.

    This formatter simply produces a list of :class:`MessageElement` objects."""

    def __init__(self):
        self._result = []

    def update(self, message):
        self._result.append(message)

    def finalize(self):
        out, self._result = self._result, []
        logger.debug('Finalizing type=%s: result=%s',
                     self.__class__.__name__, out)
        return out


no_format = SimpleFormat()


def make_format(format, format_args, request_type, *args, **kwargs):
    """Return an output formatter instance for the given request type.

    This function takes a formatter factory and instantiates a
    formatter instance for the given request type. See :ref:`output-formatters`
    for more details on formatters.

    Parameters
    ----------
        format : object
            A formatter factory or formatter instance. If this is a formatter instance,
            the remaining parameters are ignored, and `format` is returned. Otherwise,
            the `make_format()` method is called on this object to create a formatter
            instance with the remaining parameters.
        format_args : dict, ``None``
            If this is not ``None`` or empty, `format` will be called with these
            arguments before creating the output formatter.
        request_type : str
            The request type for which to create an output formatter.
        args : list
            Positional parameters to instantiate the output formatter with.
        kwargs : dict
            Keyword arguments to instantiate the output formatter with.
    """

    if format_args:
        if not callable(format):
            raise TypeError('Cannot apply format args "{}" because formatter "{}" is not callable'.format(
                ', '.join(format_args.keys()), format))
        format = format(**format_args)

    if hasattr(format, 'make_format') and callable(format.make_format):
        return format.make_format(request_type, *args, **kwargs)
    return format
