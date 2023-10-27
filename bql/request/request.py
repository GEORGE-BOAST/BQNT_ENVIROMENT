#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import collections.abc
import itertools

import six

from .items import Items
from .universe import Universe
from bqlmetadata.literals import StringLiteral, LiteralTypeError
from ..om import BqlItem, InvalidParameterValueError, Preferences


def _make_items(items):
    if isinstance(items, Items):
        return items
    elif isinstance(items, BqlItem):
        return Items(items)
    elif isinstance(items, collections.abc.Mapping):
        return Items.from_dict(items)
    elif isinstance(items, collections.abc.Sequence):
        return Items(*items)
    else:
        raise TypeError('Items must be a `bql.Items`, a `bql.BqlItem`, a '
                        'mapping, or a sequence')


def _make_universe(universe):
    if isinstance(universe, Universe):
        return universe
    elif isinstance(universe, BqlItem):
        return Universe(universe)
    elif isinstance(universe, six.string_types):
        return Universe(universe)
    elif isinstance(universe, collections.abc.Iterable):
        return Universe(*universe)
    else:
        raise TypeError('Universe must be a `bql.Universe`, a `bql.BqlItem`, '
                        'a string, or an iterable')


def _make_preferences(preferences):
    if preferences is None:
        return Preferences()
    elif isinstance(preferences, Preferences):
        return preferences
    elif isinstance(preferences, collections.abc.Mapping):
        return Preferences(**preferences)
    else:
        raise TypeError()


def _get_parameters_recursively(item, visited_items):
    params = set()
    # First get all of the parameters for this BqlItem.
    try:
        get_parameter_metadata = item._get_parameter_metadata
    except AttributeError:
        # This isn't a BqlItem, so there's nothing left to do.
        pass
    else:
        item_id = id(item)
        if item_id not in visited_items:
            # Get the parameter metadata for this item.
            params = get_parameter_metadata()
            # Now descend into the BqlItems that are bound to this BqlItem's
            # parameters.
            for value in itertools.chain(item.positional_parameter_values,
                                         item.named_parameters.values()):
                params.update(
                    _get_parameters_recursively(value, visited_items))
            visited_items.add(item_id)
        else:
            # We've already extracted parameter metadata for this item, so
            # there's no need to process it further.
            pass
    return params


def _make_with(with_params, items, universe):
    validated_with_params = {}
    if with_params:
        # Only attempt to validate parameters that are possible given
        # the Items and Universe for this request.
        params = set()
        visited_items = set()
        for item in itertools.chain(items, universe):
            params.update(_get_parameters_recursively(item, visited_items))
        # Build up a mapping of parameter name to Parameter objects (note
        # that we maintain a list because there can be more than one Parameter
        # object with the same name, and these different Parameter objects
        # may in fact validate their values differently).
        params_map = {}
        for p in params:
            params_map.setdefault(p.name, []).append(p)
            for alias in p.aliases:
                params_map.setdefault(alias, []).append(p)
        string_literal = StringLiteral()
        for (name, value) in with_params.items():
            name = name.lower()
            try:
                # Get the Parameter metadata for this with() clause parameter.
                params = params_map[name]
            except KeyError:
                # There's no metadata for this parameter, so accept the value
                # as-is without validating.
                validated_with_params[name] = string_literal.to_bql(str(value))
            else:
                # Validate the provided value against the Parameters.
                # Note that this allows BqlItems to be specified in the with()
                # clause if the parameter accepts a BqlItem, even though the
                # backend doesn't permit it (outside of range() apparently).
                # We'll let the backend deal with this case.
                for param in params:
                    try:
                        validated_value = param.validate(value)
                    except LiteralTypeError:
                        # Try to bind to the next Parameter.
                        pass
                    else:
                        # Declare success on the first Parameter we find that
                        # takes the value.
                        validated_with_params[name] = (
                            param.format_for_query_string(validated_value)
                        )
                        break
                else:
                    # The loop terminated without calling "break", so we know
                    # that all the attempted validations failed.
                    raise InvalidParameterValueError(
                        f"Parameter '{name}' in with() clause cannot "
                        f"accept value '{value}'")
        return validated_with_params


class Request(object):
    """Create a BQL Request object.

    A Request combines your universes and data items into a single 
    object in preparation to make an API call using the 
    :meth:`Service.execute` method.

    Parameters
    ----------
    universe : BqlItem or list of BqlItems
        A BQL item or list of items that describes a security universe, an
        iterable of security ticker strings, or a single security string.  The
        universe values are included in the ``FOR`` clause of the BQL query
        string.
    items : BqlItem or list of BqlItems
        A BQL data item or list of BQL data items.  The items' values are
        included in the ``GET`` clause of the BQL query string.
    with_params : dict, optional
        A dictionary that maps parameter names to values. These parameter values are
        put in the ``WITH`` clause of the BQL query string.
    preferences : dict, optional
        A mapping of preference parameter names to values.

    Example
    -------
    .. code-block:: python
        :linenos:
        :emphasize-lines: 9

        # Setup environment
        import bql
        bq = bql.Service()

        universe = bq.univ.members('INDU Index')
        data_item = bq.data.px_last(fill='prev')

        # Instantiate and execute Request
        request = bql.Request(universe, data_item)
        response = bq.execute(request)
        data = response[0].df()
        data.head()

    """
    def __init__(self, universe, items, with_params=None, preferences=None):

        if items is None:
            raise ValueError("Must specify some Items for the query request")
        if universe is None:
            raise ValueError("Must specify a Universe for the query request")
        self.__items = _make_items(items)
        self.__universe = _make_universe(universe)
        self.__unvalidated_with_params = with_params
        # self.__with is a dict {name: formatted_val} of parameter names to
        # their BQL string representations that are ready to be incorporated
        # into a BQL query string.
        self.__with = _make_with(with_params, self.__items, self.__universe)
        self.__preferences = _make_preferences(preferences).validate(
            self.__items._metadata_reader)

    def to_string(self):
        """Get the BQL query string created by this :class:`Request`.

        Returns
        -------
        str
            The BQL query string created by this :class:`Request`.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 10

            # Setup environment
            import bql
            bq = bql.Service()

            universe = bq.univ.members('INDU Index')
            data_item = bq.data.px_last(fill='prev')

            # Instantiate Request and view string
            request = bql.Request(universe, data_item)
            request.to_string()
         
        """
        components = [x for x in [self._let_clause(),
                                  self.__items.to_string(),
                                  self.__universe.to_string(),
                                  self.__format_with(),
                                  self.__format_preferences()] if x]
        return ' '.join(components)

    @property
    def items(self):
        return self.__items

    @property
    def universe(self):
        return self.__universe

    @property
    def preferences(self):
        return self.__preferences

    def _with_preferences(self, preferences):
        return Request(self.__universe,
                       self.__items,
                       with_params=self.__unvalidated_with_params,
                       preferences=preferences)

    def _without_preferences(self):
        return Request(self.__universe,
                       self.__items,
                       with_params=self.__unvalidated_with_params)

    def _with_items(self, items):
        return Request(self.__universe,
                       items,
                       with_params=self.__unvalidated_with_params,
                       preferences=self.__preferences.to_dict())

    def __repr__(self):
        return ("<Request("
                f"items={self.__items},"
                f"universe={self.__universe},"
                f"with={self.__with},"
                f"prefs={self.__preferences})>")

    def __str__(self):
        return self.to_string()

    def __format_preferences(self):
        if self.__preferences:
            string = (
                f'preferences({self.__preferences.format_for_query_string()})')
        else:
            string = ''
        return string

    def __format_with(self):
        if self.__with:
            # Print the parameters in sorted order for deterministic output.
            sorted_items = sorted(self.__with.items(), key=lambda x: x[0])
            formatted_items = ",".join([f"{k}={v}"
                                        for (k, v) in sorted_items])
            string = f'with({formatted_items})'
        else:
            string = ''
        return string

    def _let_clause(self):
        """
        Returns a string representing the LET clause to be sent to BQL.

        The LET clause is constructed from LET statememts in both the GET
        clause and the UNIVERSE specification. An empty string is returned
        if there are no LET statements.
        """
        # create a union of all LetItem uses
        let_uses = collections.OrderedDict()
        for item in itertools.chain(self.__items, self.__universe):
            if isinstance(item, BqlItem):
                # ensure let names weren't already used for something else
                for key, value in item._let_uses.items():
                    if key in let_uses and not value.equals(let_uses[key]):
                        error = f"Let name '{key}' used for multiple items"
                        raise ValueError(error)

                # all good ==> update the union
                let_uses.update(item._let_uses)

        # generate let clause from the union
        lets = ''.join(value.assignment() for value in let_uses.values())

        return f'let({lets})' if lets else ''
