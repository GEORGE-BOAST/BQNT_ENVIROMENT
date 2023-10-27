# Copyright 2019 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from collections import Counter
from collections.abc import Sequence

from .bql_item import BqlItem


def decompose_item(item):
    """Extracts the bql data items from a single bql expression.

    Parameters
    ----------
    item: bq.BqlItem
        A BqlItem.

    Returns
    -------
    A dictionary that maps BqlItem names to BqlItems.
    """

    def decompose(item):
        if not isinstance(item, BqlItem):
            return

        # To stringfy each BqlItem, we use BqlItem.format_for_query_string()
        # which is calling format_for_query_string() from the base class BqlItem.
        # We do this for cases where the item parameter is an instance of _LetItem.
        # _LetItem inherits from BqlItem and implements format_for_query_string
        # differently. In _LetItem.format_for_query_string() the let variable name
        # is returned instead of the bql expressions. The base class
        # format_for_query_string() method is used because the stringified
        # bql expression is needed to accurately maintain the seen_items set.

        stringified_item = BqlItem.format_for_query_string(item)
        if item.return_type[0] == 'ITEM' and stringified_item not in seen_items:
            field_name = item._name.lower()
            item_counter[field_name] += 1
            item_name = f'{field_name}_{item_counter[field_name]}'
            decomposed_items[item_name] = item
            seen_items.add(stringified_item)

        for param in item.parameters.values():
            if isinstance(param, Sequence):
                for sub_item in param:
                    decompose(sub_item)
            else:
                decompose(param)

    item_counter = Counter()
    seen_items = set()
    decomposed_items = {}
    decompose(item)

    return decomposed_items
