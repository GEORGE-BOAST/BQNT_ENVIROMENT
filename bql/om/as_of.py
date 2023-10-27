# Copyright 2017 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .exceptions import InvalidParameterGroupError


def _modify_as_of_date(item, date):
    return item.with_additional_parameters(as_of_date=date)


def _modify_start_end(item, date):
    return item.with_additional_parameters(
        start=date, end=date, fill='prev')


def _modify_dates_with_list(item, date):
    return item.with_additional_parameters(dates=[date])


def _modify_dates_with_single(item, date):
    return item.with_additional_parameters(dates=date)


def _inject_historical_date(item, date):
    _HISTORICAL_DATE_MODIFIERS = [
        _modify_as_of_date,
        _modify_start_end,
        _modify_dates_with_single,
        _modify_dates_with_list
    ]
    for modifier in _HISTORICAL_DATE_MODIFIERS:
        try:
            return modifier(item, date)
        except InvalidParameterGroupError:
            pass
    return item


def as_of(item, date, predicate=None):
    """Returns a copy of a BqlItem with parameters adjusted for the
    item to be "as-of" `date`.

    Only adjust BQL items in the expression tree for which the optional
    `predicate` evaluates to True.  If called with something that is not a
    `BqlItem`, such as a scalar or a list of securities, the item is returned
    unchanged (since an "as-of" date does not affect its meaning).
    """
    try:
        date = date.strftime("%Y-%m-%d")
    except AttributeError:
        pass

    try:
        old_args = item.positional_parameter_values
        old_kwargs = item.named_parameters
    except AttributeError:
        return item

    new_args = [as_of(value, date, predicate) for value in old_args]
    new_kwargs = {name : as_of(value, date, predicate)
                  for (name, value) in old_kwargs.items()}

    updated_item = item.with_parameters(*new_args, **new_kwargs)
    if predicate is None or predicate(item):
        item_as_of = _inject_historical_date(updated_item, date)
    else:
        item_as_of = updated_item
    return item_as_of
