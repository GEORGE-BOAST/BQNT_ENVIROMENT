# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import copy
import itertools
import six
from collections import OrderedDict
import logging

from .exceptions import (
    InvalidParameterGroupError,
    InvalidParameterNameError,
    ValidationError
)
from ._parameter_binding import _ParameterBinding
from .preferences import Preferences
from bqlmetadata import DataTypes, ParameterGroup, Parameter
from .as_of import as_of as as_of_fn
from .python_adaptation import strip_formatting
from .logging import deprecation_warning

_logger = logging.getLogger(__name__)


def _deprecate_preferences():
    deprecation_warning(
        _logger,
        'The binding of preferences to a BqlItem has been deprecated. '
        'Please use bq.func.applypreferences() to bind preferences'
        'to the BqlItem.',
        'bqlitem-preferences')


class BqlItem:
    # We've overridden __eq__() for constructing the BQL equals function,
    # and our own equals() test for BqlItem equality is tricky, in that
    # it can return false positives.  That complicates writing a good hash
    # function, so we're simply making BqlItem items unhashable.
    #
    # However, we do need the ability to create dictionary keys for BqlItems
    # so that we can cache BQL queries (BqlItems) already seen.  To achieve
    # this, we have a _dict_key() method (see below).
    """The implementation of a BQL universe, data item, or function. 
    
    You can chain BqlItems together or nest them inside each other to perform 
    calculations, filter universes, or otherwise transform data.

    """
    __hash__ = None

    def __init__(self,
                 name,
                 metadata,
                 output_column,
                 positional_params,
                 named_params):
        # Name that this object was instantiated with. To be used in error
        # messages, so the user can easily identify what they apply to.
        self._name = name.upper()
        # Metadata describing this object (function and/or data item).
        self._metadata = metadata
        assert len(self._metadata) > 0
        # Assume that all MetadataObjects were created from the same
        # MetadataReader.
        self._metadata_reader = metadata[0].reader
        for m in metadata:
            assert m.reader is self._metadata_reader
        # __param_bindings is a list of one or more _ParameterBinding objects
        # that validly bind to given positional_params and named_params.  If no
        # valid binding can be made, then __validate_and_bind() throws an
        # exception, so a fully constructed BqlItem always has at least one
        # item in its __param_bindings list.
        self.__param_bindings = self.__validate_and_bind(positional_params,
                                                         named_params)
        assert len(self.__param_bindings) > 0

        # Keep only those metadata for which there is a valid parameter
        # binding. We know there is at least one valid parameter binding, so we
        # should end up with at least one metadata record.
        bound_pgroups = set(pb.parameter_group
                            for pb in self.__param_bindings)
        self._metadata = [
            m for m in self._metadata
            if set(m.parameter_groups).intersection(bound_pgroups)]
        assert len(self._metadata) > 0

        # output_column can be None.  It qualifies a BQL expression with an
        # output column of interest (e.g., px_last.date).
        self._output_column = (output_column.upper()
                               if output_column is not None
                               else None)

        # The query string id of this BqlItem.  This is the hashed value
        # of the fully expanded query string of this BqlItem.
        self._query_string_id = hash(
            BqlItem.format_for_query_string(self, expand_items=True))

        # Lazily store the query string of this BqlItem.
        self._query_string = None

        # The let name ==> LetItem map that this BqlItem depends on (uses).
        self._let_uses = OrderedDict()

        # The BQL data item/func/universe func names that this BqlItem uses.
        self._bql_uses = {self._name}

        # Visit all params to properly populate the let & bql item uses.
        self._visit_params([self._append_let_uses,
                            self._append_bql_uses])

    @staticmethod
    def from_metadata(metadata, *args, **kwargs):
        # Prefer mnemonic for this item's name
        name = metadata.mnemonic
        if metadata.mnemonic is None:
            name = metadata.name

        # route via BqlItemFactory, so docstring
        # is set on the BqlItem instance.
        return BqlItemFactory(name, [metadata])(*args, **kwargs)

    @property
    def return_type(self):
        # TODO(aburgm): If return type is different for different metadatas,
        # then return DERIVED type.
        return self._metadata[0].return_type

    @property
    def preferences(self):
        _deprecate_preferences()
        return Preferences()

    def _dict_key(self):
        """
        Returns a value that can be used as a dictionary key for this BqlItem.
        """
        return self._query_string_id

    def _visit_params(self, visit_funcs):
        """Visits all params of this BqlItem and calls each of the passed
        visit_funcs functions on each BqlItem-typed param."""
        pb = self.__get_param_binding()
        for parameter in pb.parameter_group:
            params_list = pb.get_value(parameter, always_return_list=True)
            self._visit_params_list(params_list, visit_funcs)

    def _visit_params_list(self, params_list, visit_funcs):
        """Consumes the incoming params_list and calls each of the passed
        visit_func functions on each BqlItem-typed param inside it."""
        for item in params_list:
            if isinstance(item, list):
                # This happens if a parameter is a list
                self._visit_params_list(item, visit_funcs)
            elif isinstance(item, BqlItem):
                for func in visit_funcs:
                    func(item)

    def _append_let_uses(self, bql_item):
        """Appends the passed bql_item's _let_uses values into our
        _let_uses OrderedDict."""
        # ensure same let names weren't already used for something else
        for key, value in bql_item._let_uses.items():
            if key in self._let_uses and not value.equals(self._let_uses[key]):
                error = f"Let name '{key}' used for multiple values"
                raise ValueError(error)

        # all good ==> update let_uses
        self._let_uses.update(bql_item._let_uses)

    def _append_bql_uses(self, bql_item):
        """Appends the passed bql_item's _bql_uses values into our
        _bql_uses set."""
        self._bql_uses |= bql_item._bql_uses

    def _format_parameter(self, parameter, value):
        '''
        Formats the incoming parameter & parameter value pair to string
        based on parameter value type.  When the parameter value is a
        BqlItem, it formats it using the format_for_query_string() method
        inside this class.  For every other case, it uses the bqlmetadata
        Parameter's format_for_query_string() method.
        '''
        if isinstance(value, BqlItem):
            # value is either a LetItem or a BqlItem: format using our method
            return BqlItem.format_for_query_string(value, expand_items=True)
        else:
            return Parameter.format_for_query_string(parameter, value)

    def _param_to_value(self, parameter, value, expand_items):
        '''
        Returns the string or int value of the passed parameter.

        If the passed parameter is a BqlItem, and expand_items is set to True,
        then it returns its "_query_string_id" member.

        If the passed parameter isn't a BqlItem, or expand_items is set to
        False, then it returns the string obtained by calling the
        format_for_query_string() method on the passed parameter.
        '''
        if expand_items and hasattr(value, "_query_string_id"):
            # return with the existing query string id
            return value._query_string_id
        else:
            return parameter.format_for_query_string(value)

    def format_for_query_string(
            self,
            expand_items=False,
            store_query_string=False,
            add_output_column=True):
        '''
        Returns the BQL query string representation of this item.

        When expand_items is set to True, the method returns the fully
        expanded BQL query string of this object.

        When store_query_string is True, we store the method's final return
        value in a private member inside the object.

        When add_output_column is True, we add the output column names for
        items where the _output_column passed into the Constructor.
        '''

        infix_operator, prefix_operator = None, None
        if len(self._metadata) == 1:
            infix_operator = self._metadata[0].infix_operator
            prefix_operator = self._metadata[0].prefix_operator

        pb = self.__get_param_binding()
        param_values = [
            (pb.parameter_group.get_parameter(parameter),
             pb.get_value(parameter))
            for parameter in pb.parameter_group]

        if infix_operator is not None:
            assert len(param_values) == 2

            param1, param1_value = param_values[0]
            param2, param2_value = param_values[1]

            # convert to string or int
            param1_value = self._param_to_value(
                param1,
                param1_value,
                expand_items)
            param2_value = self._param_to_value(
                param2,
                param2_value,
                expand_items)

            # build return value
            string = f'({param1_value} {infix_operator} {param2_value})'
        elif prefix_operator is not None:
            assert len(param_values) == 1

            param1, param1_value = param_values[0]

            # convert to string or int
            param1_value = self._param_to_value(
                param1,
                param1_value,
                expand_items)

            # build return value
            string = f'{prefix_operator}({param1_value})'
        else:
            # TODO: Choose the name or mnemonic of the a metadata object
            # that has one of the remaining parameter groups.

            name = self._name

            # expand items if needed
            if expand_items:
                args = self.__get_param_binding().format(
                    self._format_parameter)
            else:
                args = self.__get_param_binding().format_for_query_string()
            string = f"{name}({args})"

        # Add output column specifier.
        if add_output_column and self._output_column is not None:
            assert string.endswith(")")
            string = f"{string}.{self._output_column}"

        # Store the query string lazily, when we are asked to.
        if store_query_string:
            self._query_string = string

        return string

    def equals(self, other):
        # Test two BqlItems for equivalence.
        #
        # We've overridden __eq__() for purposes of building OM expressions, so
        # use equals() if you want to determine if this BqlItem is equivalent
        # to some "other" BqlItem.
        #
        # What makes one BqlItem equal to another BqlItem is tricky.  Ideally
        # you'd want equal BqlItems to always return the same query results,
        # but that's dependent on how the backend processes the query strings
        # that come from these BqlItems.  It's complicated because some data
        # item names and function names are ambiguous--they map onto multiple
        # data sources, and it's up to the backend to resolve the ambiguity.
        # We on the client side aren't privvy to the rules the backend uses
        # to do this, so we can only go so far when it comes to testing
        # for equality.
        #
        # In short we err on the side saying that two BqlItems *might* be
        # equal, allowing for false positives.  Here's a sketch of how we
        # decide that two BqlItems are equal, starting with some structure
        # behind the BqlItems:
        #
        # * You construct a BqlItem with a name.  This name can be the
        #   canonical name for a data item or function, a mnemonic, or an
        #   alias.
        #
        # * Some names map onto multiple data items or functions, meaning that
        #   some BqlItems reference more than one metadata record.
        #
        # * If the two BqlItems A and B each only reference one metadata
        #   record, then A and B each unambiguously refers to the one data item
        #   or function in the backend.  And these two metadata records must
        #   be the same for A and B to be considered equal.
        #
        # * If either A or B have multiple metadata records, then there is
        #   ambiguity regarding what data item or function is intended.  In
        #   the spirit of equals() returning True if A and B *might* be equal,
        #   we check to see if A and B share any metadata records in common;
        #   if they don't then we know they're not equal.
        #
        # * Once we've established common metadata--meaning that the two
        #   BqlItems might point to the same data item or function on the
        #   backend--we need to deal with the parameter values specified to
        #   the BqlItems at construction time.
        #
        # * A and B must have the same parameter values to be considered equal.
        #
        # * This is relatively straightforward, although there is one subtle
        #   point that has to do with default values.  Let's say A and B are
        #   aliases and take one optional parameter "foo" that has a default
        #   value of 1.  These two BqlItems should be considered equal:
        #
        #     a = A()
        #     b = B(foo=1)
        #
        #   B explicitly provides the same value for foo that would otherwise
        #   be inferred from the foo's default value.
        #
        if other is not None and isinstance(other, BqlItem):
            if self is other:
                equals = True
            else:
                # Examine the _ParameterBinding for those ParameterGroups from
                # metadata that are shared between both "self" and "other".
                equals = self.__are_param_bindings_equal(other)
        else:
            equals = False
        return equals

    def _with_preferences(self, preferences):
        return self._without_preferences()

    def _without_preferences(self):
        _deprecate_preferences()
        return BqlItem(self._name,
                       self._metadata,
                       self._output_column,
                       self.positional_parameter_values,
                       self.named_parameters)

    def _with_outputcol(self, output_column):
        return BqlItem(self._name,
                       self._metadata,
                       output_column,
                       self.positional_parameter_values,
                       self.named_parameters)

    def _get_parameter_metadata(self):
        # Return the Parameter objects for all feasible ParameterGroups,
        # regardless of whether the Parameter is bound to a value.
        params = set()
        for metadata in self._metadata:
            for pg in metadata.parameter_groups:
                params.update(pg.values())
        return params

    def __repr__(self):
        return f"<{self.__class__.__name__}{self._param_string()}{self._out_col_string()}>"

    def _out_col_string(self):
        return f"[{self._output_column!r}]" if self._output_column else ""

    def _param_string(self):
        return "(name=%s,%s)" % (
            self._name,
            self.__get_param_binding().format(lambda param, val: str(val))
        )

    def __str__(self):
        return self.format_for_query_string()

    def __deepcopy__(self, memo_dict):
        return BqlItem(self._name,
                       self._metadata,
                       self._output_column,
                       copy.deepcopy(self.positional_parameter_values,
                                     memo_dict),
                       copy.deepcopy(self.named_parameters, memo_dict))

    # SPA: hack to force a fail of the Parameter.__is_list
    # defining __getitem__ no longer raises a TypeError
    # during invocations of the `iter` method on instances
    # of BqlItem; so we raise the TypeError by hand here to
    # ensure the expected behavior.
    def __iter__(self):
        raise TypeError("'BqlItem' object is not iterable")

    def __getitem__(self, item):
        # Creates a new BqlItem that specifies an output column.
        if not isinstance(item, six.string_types):
            raise TypeError("Output column must be a string")
        return self._with_outputcol(item)

    def __getattr__(self, name):
        # This function can be called in two different scenarios:
        #
        # 1) To apply a function to this object, returning a new BqlItem:
        #
        #       a = obj.avg()
        #
        # 2) To retrieve the value of the specified parameter for this object:
        #
        #       start_date = obj.start
        #
        # behave normally when requesting dundermethods
        if name.startswith('__'):
            raise AttributeError(name)
        name = strip_formatting(name)

        metadata = (
            self._metadata_reader.get_functions(name) or
            self._metadata_reader.get_data_items(name) or
            self._metadata_reader.get_universe_handlers(name)
        )

        if metadata:
            return BqlItemFactory(name, metadata, [self])

        # Deprecating scenario #2, retrieving a value of a parameter by
        # chaining the parameter on the end of the object like a
        # property accessor
        try:
            value = self.__get_param_value(name)

        # keep error raised backwards compatible
        except InvalidParameterNameError:
            raise AttributeError(
                f"Invalid parameter, function or dataitem name '{name}'")

        else:
            is_bound_parameter = (name in self.parameters)
            payload = {
                'parameter_name': name,
                'bound_parameter': is_bound_parameter
            }
            deprecation_warning(
                _logger,
                f"The 'obj.{name}' syntax for parameters is being deprecated.  "
                f"Please use 'obj.parameters['{name}']' to access the value "
                "of a bound parameter instead",
                'bql-parameters',
                payload=payload
            )
            return value

    def __setattr__(self, name, value):
        # Disallow setting of properties on the object. Someone who tried to do
        # that might think that this allows to change the value of parameters,
        # which it doesn't.
        # Internal properties starting with underscores are OK.
        if name.startswith('_'):
            super(BqlItem, self).__setattr__(name, value)
        else:
            raise AttributeError(
                f'Cannot set attribute "{name}" on "{self.__class__.__name__}"')

    @property
    def description(self):
        return self._metadata[0].description

    @property
    def parameters(self):
        """dict: A dictionary of parameters constructed from the 
        ``positional_params`` and ``named_params`` arguments given to this 
        :class:`BqlItem`.

        """
        params = {}
        for (name, value) in self.positional_parameters:
            params.setdefault(name, []).append(value)
        # If a parameter is bound to only one value, then return the single
        # value instead of a list of one item.
        params = {name: values[0] if len(values) == 1 else values
                  for (name, values) in params.items()}
        for (name, value) in self.named_parameters.items():
            assert name not in params
            params[name] = value
        return params

    @property
    def positional_parameters(self):
        """Return the positional parameters and their values for this item.

        Returns a list of (name, value) tuples specifying the positional
        parameters that this item was constructed with.  Note that the same
        parameter name can appear multiple times in the returned list in
        the case of VARARGs.
        """
        pos_params = self.__get_param_binding().positional_parameters
        return [(param.name, value) for (param, value) in pos_params]

    @property
    def positional_parameter_values(self):
        """Return a list of parameter values specified positionally."""
        return [value for (_, value) in self.positional_parameters]

    @property
    def named_parameters(self):
        """Return the named parameters and their values for this item.

        Returns a dictionary specifying the named parameters and names that
        this item was constructed with.
        """
        return self.__get_param_binding().named_parameters

    def __get_param_value(self, param_name):
        # Find the first parameter group that has param_name.  If none can be
        # found, then param_name isn't valid.
        for binding in self.__param_bindings:
            try:
                return binding.get_value(param_name)
            except InvalidParameterNameError:
                # This parameter group doesn't have the parameter, so try the
                # next one.
                pass
        raise InvalidParameterNameError(
            f"'{param_name}' is not a valid parameter for {self._name}")

    def __validate_and_bind(self, positional_params, named_params):
        # Find valid parameter groups given a set of parameters.

        # Raises
        # InvalidParameterGroupError
        #       Supplied parameters are incompatible with all applicable
        #       ParameterGroups.

        # Returns
        # List of _ParameterBinding
        #       All possible valid parameter bindings for this item given
        #       positional parameters and named parameters.

        # Get combined list of parameter groups from all metadata objects,
        # and remove duplicates.  When the BqlItem has only one metadata
        # object (the usual case), then the parameter groups are listed
        # in the order they appear in the metadata.  If there are multiple
        # metadata objects, then we can't guarantee a total order among
        # the parameter groups--it depends on the order in which the
        # metadata objects are listed.  This suggests RDSIBQUN-2914 may
        # surface again in this case, at which point we'll need to somehow
        # establish a priority order among the multiple parameter groups.
        param_groups = []
        unique_param_groups = set()
        for metadata in self._metadata:
            for pgroup in metadata.parameter_groups:
                if pgroup not in unique_param_groups:
                    unique_param_groups.add(pgroup)
                    param_groups.append(pgroup)

        if not param_groups:
            # This object doesn't take any parameters.

            # TODO: Should this always through? After all, this means that we
            # don't have a parameter group, not even "EMPTY_PG".
            if not positional_params or not named_params:
                return [_ParameterBinding(ParameterGroup())]
            else:
                raise InvalidParameterGroupError(self._name, [], [])
        elif len(param_groups) == 1:
            # There is only one ParameterGroup, so it must be the one we
            # validate against.
            try:
                return [_ParameterBinding(param_groups[0],
                                          positional_params,
                                          named_params)]
            except ValidationError as ex:
                raise InvalidParameterGroupError(
                    self._name, [param_groups[0]], [ex])
        else:
            # Look for all the ParameterGroups that match our arguments.  There
            # may be several, which in practice isn't a big deal because the
            # backend will always be able to disambiguate even if only some of
            # the parameters are specified at construction time: see BQUN-418
            # for more details.
            all_param_bindings = []
            errors = []
            for pg in param_groups:
                try:
                    bindings = _ParameterBinding(pg,
                                                 positional_params,
                                                 named_params)
                except ValidationError as ex:
                    # This parameter group doesn't match the arguments given.
                    errors.append(ex)
                else:
                    all_param_bindings.append(bindings)

            if not all_param_bindings:
                raise InvalidParameterGroupError(
                    self._name, param_groups, errors)

            return all_param_bindings

    def __are_param_bindings_equal(self, other):
        assert other is not None
        assert isinstance(other, BqlItem)
        equals = False
        # Only examine _ParameterBinding for the specified parameter groups
        # that are common between the "self" and "other" metadata.  If the two
        # items don't share any common metadata, then we know that the two
        # can't be equal.
        common_metadata = set(self._metadata).intersection(other._metadata)
        if common_metadata:
            # Restrict ourselves to looking only at the _ParameterBinding for
            # ParameterGroups that come from the common metadata.
            common_param_groups = set()
            for m in common_metadata:
                common_param_groups.update(m.parameter_groups)
            # Make a map of _ParameterBinding indexed by ParameterGroup for
            # easy lookups.
            self_bindings_by_pgroup = {
                pb.parameter_group: pb
                for pb in self.__param_bindings
                if pb.parameter_group in common_param_groups
            }
            other_bindings_by_pgroup = {
                pb.parameter_group: pb
                for pb in other.__param_bindings
                if pb.parameter_group in common_param_groups
            }
            # Note that these two maps do not necessarily have identical keys.
            # common_param_groups was determined strictly from the metadata
            # of "self" and "other".  They represent *potential* parameter
            # bindings.  It doesn't take into account whether the parameter
            # values actually bind to the parameter group.
            #
            # But we know that in order for the two BqlItems to be considered
            # equal, their parameter values must agree.  That means if there is
            # a ParameterGroup from common_param_group that binds successfully
            # in the first BqlItem, it must bind successfully in the second
            # BqlItem for the two to be considered equal.
            for pgroup in common_param_groups:
                try:
                    self_param_binding = self_bindings_by_pgroup[pgroup]
                    other_param_binding = other_bindings_by_pgroup[pgroup]
                except KeyError:
                    # pgroup is bound in one but not the other object, so skip
                    # it and move to the next ParameterGroup.
                    pass
                else:
                    if self_param_binding == other_param_binding:
                        equals = True
                        break
        return equals

    # TODO: We could potentially generate these
    # Note that, for reverse operators, `other` could be a scalar, so we
    # cannot just do return other.__XXX__(self)

    # Comparison operators
    def __eq__(self, other):
        return self.__binary_operator('==', self, other)

    def __ne__(self, other):
        return self.__binary_operator('!=', self, other)

    def __lt__(self, other):
        return self.__binary_operator('<', self, other)

    def __gt__(self, other):
        return self.__binary_operator('>', self, other)

    def __le__(self, other):
        return self.__binary_operator('<=', self, other)

    def __ge__(self, other):
        return self.__binary_operator('>=', self, other)

    # Arithmetic operators
    def __add__(self, other):
        return self.__binary_operator('+', self, other)

    def __radd__(self, other):
        return self.__binary_operator('+', other, self)

    def __sub__(self, other):
        return self.__binary_operator('-', self, other)

    def __rsub__(self, other):
        return self.__binary_operator('-', other, self)

    def __mul__(self, other):
        return self.__binary_operator('*', self, other)

    def __rmul__(self, other):
        return self.__binary_operator('*', other, self)

    # Python 2
    def __div__(self, other):
        return self.__binary_operator('/', self, other)

    def __rdiv__(self, other):
        return self.__binary_operator('/', other, self)

    # Python 3
    def __truediv__(self, other):
        return self.__binary_operator('/', self, other)

    def __rtruediv__(self, other):
        return self.__binary_operator('/', other, self)

    def __pow__(self, other):
        return self.__binary_operator('^', self, other)

    def __rpow__(self, other):
        return self.__binary_operator('^', other, self)

    def __neg__(self):
        metadata = self._metadata_reader.get_by_operator('-', 'prefix')
        assert metadata is not None
        output_column = None
        return BqlItem(metadata.name,
                       [metadata],
                       output_column,
                       [self],
                       named_params={})

    def __binary_operator(self, symbol, arg1, arg2):
        metadata = self._metadata_reader.get_by_operator(symbol, 'infix')
        assert metadata is not None
        output_column = None
        return BqlItem(metadata.name,
                       [metadata],
                       output_column,
                       [arg1, arg2],
                       named_params={})

    def __get_param_binding(self):
        # self.__param_bindings contains a list of possible bindings to
        # different parameter groups.  The fact that there are possibly many
        # such bindings isn't important to us here, because all of the
        # arguments specified during construction have been validly bound to
        # their parameters in each of the __param_bindings.  That means the
        # parameter values are the same for each __param_bindings, so we
        # just need to look at the first one when looking up parameter
        # values.
        return self.__param_bindings[0]

    def with_parameters(self, *args, **kwargs):
        """Create a copy of this :class:`BqlItem` with existing parameters 
        replaced by the given positional arguments (``args``) and named 
        arguments (``kwargs``).

        Returns
        -------
        BqlItem
            A new BqlItem with the updated parameter set.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 8

            # Setup environment
            import bql
            bq = bql.Service()

            data_item = bq.data.px_last(currency='USD', ca_adj='full')

            # Ignore original parameters
            data_item = data_item.with_parameters(currency='EUR', fill='prev')
            data_item.parameters

        """
        # LetItem.with_parameters() introduces a _retain_let parameter, but
        # that needs to be ignored here when building the real BqlItem.
        if '_retain_let' in kwargs:
            del kwargs['_retain_let']
        return BqlItem(self._name,
                       self._metadata,
                       self._output_column,
                       args,
                       kwargs)

    def with_additional_parameters(self, **new_parameters):
        """Create a copy of this :class:`BqlItem` with existing parameters 
        supplemented by the given keyword arguments in ``new_parameters``.

        Conflicts between new parameters and the existing parameters 
        are resolved by retaining the existing ones.

        Returns
        -------
        BqlItem
            A new BqlItem with the updated parameter set.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 8

            # Setup environment
            import bql
            bq = bql.Service()

            data_item = bq.data.px_last(fill='prev', dates='-1D')

            # Add a currency parameter
            data_item = data_item.with_additional_parameters(currency='EUR')
            data_item.parameters

        """
        new_parameters = _ParameterBinding.casefold(new_parameters)
        old_arg_names = set(name for (name, _) in self.positional_parameters)
        old_arg_values = self.positional_parameter_values
        old_kwargs = self.named_parameters

        new_kwargs = {
            param: value
            for (param, value) in new_parameters.items()
            if param not in old_arg_names
        }

        new_kwargs.update(old_kwargs)

        # Reconstruct the object using the existing positional arguments as
        # well as the augmented named arguments.  This reconstruction will be
        # based off the state of a particular possible parameter group selected
        # for this item.
        return self.with_parameters(*old_arg_values, **new_kwargs)

    def with_updated_parameters(self, **new_parameters):
        """Create a copy of this :class:`BqlItem` with existing parameters 
        supplemented by the given keyword arguments in ``new_parameters``.
        
        Conflicts between new parameters and the existing parameters 
        are resolved by adopting the new parameters.

        Returns
        -------
        BqlItem
            A new BqlItem with the updated parameter set.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 8

            # Setup environment
            import bql
            bq = bql.Service()

            data_item = bq.data.px_last(currency='EUR', fill='prev')

            # Override previous parameters and add new ones
            data_item = data_item.with_updated_parameters(currency='USD', dates='0D')
            data_item.parameters

        """
        new_parameters = _ParameterBinding.casefold(new_parameters)
        positional = OrderedDict(self.positional_parameters)
        new_kwargs = dict(self.named_parameters)

        for name, value in new_parameters.items():
            if name in positional:
                positional[name] = value
                continue

            new_kwargs[name] = value

        return self.with_parameters(*positional.values(), **new_kwargs)

    def let(self, name=None):
        """Create a new Let Item (:class:`_LetItem`) for this :class:`BqlItem`.

        Let Items are PyBQL's implementation of `let()`, an optional BQL clause
        that allows you to define local variables in your query. You can use 
        these variables to simplify other clauses.

        Parameters
        ----------
        name : str
            The name of this :class:`_LetItem`.

        Returns
        -------
        _LetItem
            Class to name BqlItems for ease of use/shorthand notation.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 7

            # Setup environment
            import bql
            bq = bql.Service()

            # Instantiate BqlItem
            data_item = bq.data.px_last()
            data_item = data_item.let('last_price')

            # Instantiate Request and view query string
            request = bql.Request('AAPL US Equity', data_item)
            request.to_string()
         
        """
        from .let import let
        return let(name, self)

    # imported method definitions
    as_of = as_of_fn


class BqlItemFactory(object):
    def __init__(self, name, metadata, bound_args=[]):
        self.__name = name
        self.__metadata = metadata
        self.__bound_args = bound_args

        assert len(self.__metadata) > 0
        if len(self.__metadata) == 1:
            self.__doc__ = self.__metadata_description(self.__metadata[0])
        else:
            descriptions = '\n\n'.join(
                f'{metadata.mnemonic}\n'
                f'{"=" * len(metadata.mnemonic)}\n'
                f'{self.__metadata_description(metadata)}'
                for metadata in self.__metadata)
            self.__doc__ = (
                f'This item with name "{self.__name}" can refer to '
                f'multiple entities:\n\n{descriptions}')

    def __metadata_description(self, metadata):
        # Returns the description of a single metadata item.
        # First, give the signature(s), then the description, then the
        # parameter descriptions.
        signatures = '\n'.join(self.__metadata_signature(metadata, pgroup)
                               for pgroup in metadata.parameter_groups)
        seen_params = set()
        parameters = [param
                      for pgroup in metadata.parameter_groups
                      for param in pgroup.values()
                      if not (param in seen_params or seen_params.add(param))]
        param_descriptions = '\n'.join(
            self.__metadata_parameter_description(param)
            for param in parameters)

        if not metadata.description:
            description = ""
        else:
            description = metadata.description.strip() + '\n\n'

        return f'{signatures}\n\n{description}Parameters:\n{param_descriptions}'

    def __metadata_signature(self, metadata, pgroup):
        name = self.__name.upper()
        params = ', '.join(self.__metadata_signature_parameter(parameter)
                           for parameter in pgroup.values())
        return f'{name}({params})'

    def __metadata_signature_parameter(self, parameter):
        result = parameter.name.upper()
        if parameter.default_value is not None:
            result = (f'{result}='
                      f'{parameter.format_for_docstring(parameter.default_value)}')

        if not parameter.is_optional and parameter.default_value is None:
            result = f'{result} <mandatory>'
        return result

    def __metadata_parameter_description(self, parameter):
        name = parameter.name.upper()
        param_type = parameter.data_type[0]
        description = parameter.description
        result = f'{name} ({param_type}): {description}'
        if parameter.data_type == DataTypes.ENUM:
            whitespace = ' ' * (
                len(parameter.name.upper()) + len(parameter.data_type[0]) + 5)
            values = '", "'.join(parameter.literal.enumerants)
            result = f'{result}\n{whitespace}Possible values are: "{values}".'
        return result

    def __call__(self, *args, **kwargs):
        output_column = None
        item = BqlItem(self.__name,
                       self.__metadata,
                       output_column,
                       list(itertools.chain(self.__bound_args, args)),
                       kwargs)
        # Propagate documentation to the instantiated item.
        item.__doc__ = self.__doc__
        return item
