#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import collections.abc
import itertools

from bqlmetadata.literals import DataTypes
from ..om import BqlItem, ValidationError


class InvalidItemsError(ValidationError):
    pass


class Items(collections.abc.Sequence):
    def __init__(self, *unnamed_items, **named_items):
        # Specify the items to be returned from the query request.

        # `unnamed_items` and `named_items` collectively define a list of OM
        # items that you want to query.  The response you get from executing
        # this query request will contain multiple `SingleItemResponse`
        # objects, one for each item in `Items`.  There is some order
        # applied to these returned `SingleItemResponses`:

        # * The query results for unnamed items will be first, in the order
        # specified in the unamed_items argument.

        # * The query results for named items will be next in an arbitrary
        # order.  You're able to look up the `SingleItemResponse` by name
        # from the query response.

        # By default the value column in each `SingleItemResponse` takes its
        # name from the expanded query string for the corresponding OM item.
        # These names can become quite long, depending on how complex the
        # expression is.  Named items get around this, by allowing you to
        # provide your own names to be used for in `SingleItemResponse`.
        if not unnamed_items and not named_items:
            raise InvalidItemsError("Must specify at least one item to query")

        for item in itertools.chain(unnamed_items, named_items.values()):
            if (not isinstance(item, BqlItem) or
                    item.return_type == DataTypes.UNIVERSE):
                raise InvalidItemsError('Only functions and data items are'
                                        ' allowed as queryable BQL items.')

        # Establish an order for the items, storing (name, item) tuples.
        self.__items = list(itertools.chain(
            # Unnamed items come first.
            [(None, item) for item in unnamed_items],
            # Followed by named items in an arbitrary order.
            named_items.items()))
        # Lazily evaluate a name -> item map when we need it.
        self.__named_items = None
        # Remember the MetadataReader that created these items.  We only use
        # the MetadataReader to validate Preferences bound to a Request; and
        # for this we only need a MetadataReader that knows about the
        # applyPreferences() function.
        self._metadata_reader = None
        for (_, item) in self.__items:
            if item._metadata_reader._get_apply_preferences() is not None:
                self._metadata_reader = item._metadata_reader
                break
        assert self._metadata_reader is not None

    def __getitem__(self, idx):
        return self.__items[idx][1]

    def __iter__(self):
        # Iterate through the items, starting with the unnamed items.
        return iter([item for (_, item) in self.__items])

    def __len__(self):
        return len(self.__items)

    @staticmethod
    def from_dict(named_items):
        # Create an `Items` object from a mapping of name to OM items.

        # This is an alternative to creating the object through the *args and
        # **kwargs constructor.  Note that if `named_items` is a regular
        # `dict`, then the order of the items is unspecified.  Keep this in
        # mind when iterating through the `SingleItemReponse` objects that
        # come back in the response to the BQL query request.
        if None in named_items:
            raise ValueError("All items must have a name")
        obj = Items(**named_items)
        # Override the ordering, just in case "named_items" is an OrderedDict.
        obj.__items = list(named_items.items())
        return obj

    def get(self, name):
        # Return the item by its name.
        if name is None:
            raise ValueError("Must specify a name")
        if self.__named_items is None:
            self.__named_items = {
                name: item for (name, item) in self.__items if name is not None
            }
        return self.__named_items[name]

    def to_tuple_list(self):
        # Return a list of (name, item) tuples.

        # A tuple is returned for all items; if an item is unnamed, then "name"
        # is None in its tuple.  Unnamed items appear first in the list.
        return self.__items

    def to_string(self):
        data_items = ','.join(self.__format_for_query_string(item)
                              for _, item in self.__items)
        return f'get({data_items})'

    @staticmethod
    def __format_for_query_string(item):
        return (item._query_string
                if item._query_string
                else item.format_for_query_string(store_query_string=True))
