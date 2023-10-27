#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import collections.abc
import logging

import six

from bqmonitor.permissions import is_enabled_for_bqmonitor
from bqlmetadata.literals import DataTypes, LiteralTypeError, StringLiteral
from ..om import BqlItem, ValidationError


logger = logging.getLogger(__name__)


class InvalidUniverseError(ValidationError):
    pass


class Universe(collections.abc.Sequence):
    def __init__(self, *items):
        if not items:
            raise InvalidUniverseError(
                "Must specify at least one security or universe to query")
        # Validate the items.  Each item must be a single string, list of
        # strings, or universe handler.
        self.__literal = StringLiteral()
        self.__items = []
        self.__has_ticker = False
        self.__has_universe_handler = False
        for item in items:
            if isinstance(item, BqlItem):
                # Only accept universe handlers, not other types of BqlItems.
                if item.return_type != DataTypes.UNIVERSE:
                    raise InvalidUniverseError(
                        f'{item._name} is not a universe handler')
                else:
                    self.__has_universe_handler = True
                self.__items.append(item)
            else:
                # Allow single strings as a special case.
                if isinstance(item, six.string_types):
                    item = [item]
                try:
                    iterator = iter(item)
                except TypeError:
                    raise InvalidUniverseError(
                        'Universe item must be a list of strings')
                try:
                    self.__items.extend([self.__literal.validate(v)
                                        for v in iterator])
                except LiteralTypeError as ex:
                    raise InvalidUniverseError(str(ex))
                else:
                    self.__has_ticker = True
        # Per RDSIBQUN-3007 only lists of tickers are allowed.
        if len(self.__items) > 1 and self.__has_universe_handler:
            raise InvalidUniverseError("Universe must be a single ticker, list "
                                       "of tickers, or single universe handler")

    def __len__(self):
        return len(self.__items)

    def __getitem__(self, idx):
        return self.__items[idx]

    def __iter__(self):
        return (item for item in self.__items)

    def to_string(self):
        return f"for({self.__format_items_for_query_string()})"

    def __format_items_for_query_string(self):
        assert len(self.__items) > 0
        items = (self.__format_for_query_string(obj) for obj in self.__items)
        # for bqmonitor to be able to consistently hash bql requests,
        # we need our list of param elements to be deterministic in ordering
        # hence we sort the elements in the list on formatting
        if is_enabled_for_bqmonitor():
            items = sorted(items)
        items_as_string = ','.join(items)
        # Only put brackets around a list of items in the for() clause,
        # unless we have a single ticker (even a single ticker needs
        # to be in a list).
        if len(self.__items) == 1 and not self.__has_ticker:
            string = f"{items_as_string}"
        else:
            string = f"[{items_as_string}]"
        return string

    def __format_for_query_string(self, obj):
        if isinstance(obj, BqlItem):
            string = obj.format_for_query_string()
        else:
            # By design obj must be a validated string if not a BqlItem.
            string = self.__literal.to_bql(obj)
        return string
