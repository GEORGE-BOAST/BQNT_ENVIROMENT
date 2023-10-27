# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import collections
import numpy as np

from .exceptions import (MissingParameterError, InvalidParameterNameError,
                         InvalidParameterValueError, UnexpectedParameterError)
from bqlmetadata import LiteralTypeError, Parameter


def is_nan(val):
    try:
        return np.isnan(val)
    except TypeError:
        # np.isnan() throws an exception for value types that it doesn't
        # recognize (like strings).  These are not NAs.
        return False


class _ParameterBinding(object):
    # We've overloaded __eq__() in a way that different _ParameterBinding
    # objects can be considered equal even if the __param_values dicts
    # are different.  That makes it difficult to implement a hash.
    __hash__ = None

    def __init__(self, parameter_group, positional_params, named_params):
        # parameter_group is a specific parameter signature for an BqlItem:
        # it specifies all the parameters that the BqlItem can take.  Our
        # job here is to bind the provided parameters to those in the
        # parameter_group.
        self.__param_group = parameter_group
        # Validate the positional and named parameters.  The __validate()
        # function throws an exception if the arguments don't agree with
        # the specified ParameterGroup.
        #
        # * self.__positional_parameters is a [(Parameter, value)] list.
        #   A parameter can appear several times in the case of VARARGs.
        #
        # * self.__named_parameters is an ordered {param_name: value} map.
        #   param_name is either the formal parameter name or parameter alias
        #   that the user specified in named_params.
        #
        (self.__positional_parameters, validated_named_parameters) = (
            self.__validate(parameter_group, positional_params, named_params))
        self.__named_parameters = collections.OrderedDict(
            validated_named_parameters.values())
        # Put all parameters in a {Parameter: [values]} map (note the list
        # of values per Parameter to accommodate VARARGs).
        self.__param_values = collections.OrderedDict()
        for (param, value) in self.__positional_parameters:
            self.__param_values.setdefault(param, []).append(value)
        for (param, (_, value)) in validated_named_parameters.items():
            # self.__validate() prevents a parameter from being specified both
            # in positional_params and named_params.
            assert param not in self.__param_values
            self.__param_values[param] = [value]

    def __repr__(self):
        return "<ParameterBindings(%s,%s)>" % (repr(self.__param_group),
                                               str(self))

    def __str__(self):
        return str({p.name: v for (p, v) in self.__param_values.items()})

    def __len__(self):
        return len(self.__param_values)

    def __eq__(self, other):
        if not isinstance(other, _ParameterBinding):
            return False
        if self.__param_group != other.__param_group:
            return False

        # Look at all the parameters bound in these two _ParameterBinding.
        # Their corresponding values must be the same, taking into account that
        # one may have a parameter explicitly bound while the other's value is
        # implicitly given through its default value. get_value() takes into
        # account default values for unbound parameters.
        all_param_names = set(self.__param_group).union(other.__param_group)
        return all(_ParameterBinding.__are_param_values_equal(
                        self.get_value(param_name),
                        other.get_value(param_name))
                   for param_name in all_param_names)

    def __ne__(self, other):
        return not (self == other)

    def get_value(self, param_name, always_return_list=False):
        # Return the value or values bound to a parameter.

        # `param_name` can be either the name of the parameter or an alias of
        # the parameter, regardless of how the user specified the parameter at
        # time of construction.

        # Also note that this function will return a list of values in the case
        # of a VARARG parameter specified more than once; otherwise, by default
        # a single value is returned.  If you'd rather return a list even in the
        # single-value case for consistency, then specify always_return_list=True.
        param = self.__param_group.get_parameter(param_name)
        if param is not None:
            try:
                values = self.__param_values[param]
            except KeyError:
                # This parameter does not have a value bound to it, so use its
                # default value.
                values = [param.default_value]
        else:
            raise InvalidParameterNameError(
                f"No parameter named '{param_name}' in parameter group")
        return (values[0] if len(values) == 1 and not always_return_list
                else values)

    def format(self, parameter_value_formatter):
        # Positional arguments are emitted first in order.
        arguments = [
            parameter_value_formatter(p, v)
            for (p, v) in self.positional_parameters
        ]
        # Named arguments are next, emitted in the order they appear in the
        # parameter group (not the order they were specified at construction
        # time).
        named_arguments = {
            self.__param_group.get_parameter(bound_name): (bound_name, v)
            for (bound_name, v) in self.named_parameters.items()
        }
        for param in self.__param_group.values():
            try:
                (bound_name, val) = named_arguments[param]
            except KeyError:
                # No value bound to this parameter.
                pass
            else:
                arguments.append(
                    f"{bound_name}={parameter_value_formatter(param, val)}")
        return ",".join(arguments)

    def format_for_query_string(self):
        return self.format(Parameter.format_for_query_string)

    @property
    def parameter_group(self):
        return self.__param_group

    @staticmethod
    def __are_param_values_equal(v1, v2):
        # If we're comparing BqlItems, then we need to use BqlItem.equals().
        try:
            equals = v1.equals
        except AttributeError:
            # v1 doesn't look like a BqlItem.
            pass
        else:
            return equals(v2)
        # How about v2?
        try:
            equals = v2.equals
        except AttributeError:
            # v2 doesn't look like a BqlItem either.  Just use regular __eq__().
            return (
                (v1 == v2) or
                # Treat np.nan as a special case, since np.nan != np.nan.
                (is_nan(v1) and is_nan(v2))
            )
        else:
            return equals(v1)

    def __validate_parameter(self, param, val):
        try:
            return param.validate(val)
        except LiteralTypeError as ex:
            # Turn type errors into InvalidParameterValueErrors, so that
            # BqlItem tries to find a better parameter binding for its set
            # of parameters.
            raise InvalidParameterValueError(str(ex))

    def __validate_positional_args(self, parameter_group, args):
        # Determine the parameter names for each of the positional arguments,
        # which follows the order given in the parameterGroup metadata.
        validated_param_values = []
        all_params = list(parameter_group.values())
        param_index = 0
        arg_index = 0
        while arg_index < len(args) and param_index < len(all_params):
            param = all_params[param_index]
            val = args[arg_index]
            try:
                validated_val = self.__validate_parameter(param, val)
            except InvalidParameterValueError:
                # Usually an InvalidParameterValueError is a true validation
                # error that we want to propagate to the caller.  But it may
                # also be that we've been validating a VARARG parameter and
                # now "val" legitimately binds to the next parameter in
                # all_params.  This would mean that a VARARG is not the last
                # parameter in the parameter group; although unconventional,
                # we're allowing it here just in case BQL throws us a
                # curveball later.
                if param.is_vararg():
                    param_index += 1
                    # Keep arg_index unchanged, because we want to validate
                    # the current arg against the next parameter.
                else:
                    raise
            else:
                # If param is a VARARG, then we want to keep validating *args
                # against it until we run out of *args or find a argument that
                # doesn't validate against it (suggesting it may bind to the
                # parameter that follows the VARARG).
                if not param.is_vararg():
                    param_index += 1
                # val has been validated against param, so move to the the
                # next argument.
                arg_index += 1
                validated_param_values.append((param, validated_val))
        # Are there any *args that haven't been bound?
        if arg_index < len(args):
            raise UnexpectedParameterError("Too many arguments")
        return validated_param_values

    def __validate_named_args(self, parameter_group, named_args):
        validated_param_values = collections.OrderedDict()
        for (name, value) in named_args.items():
            param = parameter_group.get_parameter(name)
            if param is not None:
                if param in validated_param_values:
                    raise UnexpectedParameterError(
                        f"'{param.name}' specified more than once through "
                        "its name and/or aliases")
                else:
                    validated_param_values[param] = (
                        # Associate the name specified by the user (either
                        # parameter name or alias) with the parameter value.
                        name, self.__validate_parameter(param, value)
                    )
            else:
                raise InvalidParameterNameError(
                    "'%s' is not a valid parameter name" % name)
        return validated_param_values

    def __validate(self, parameter_group, args, named_args):
        # We've standardized on lowercase parameter names.
        named_args = _ParameterBinding.casefold(named_args)
        # First bind the positional arguments if any.  Note that the same
        # arameter can have multiple values in the case of VARARGs.
        positional_parameters = self.__validate_positional_args(
            parameter_group, args)
        # Make sure that any of the named arguments haven't already been
        # bound through the positional arguments--that would mean the user
        # specified the same argument twice.
        for (p, _) in positional_parameters:
            if p.name in named_args:
                raise UnexpectedParameterError(
                    f"'{p.name}' specified both as a positional and named "
                    "parameter")
            for alias in p.aliases:
                if alias in named_args:
                    raise UnexpectedParameterError(
                        f"'{alias}' specified both as a positional and named "
                        "parameter")
        # Now fold the named arguments into the final parameter bindings.  We
        # also store the (processed and converted) named parameters.
        named_parameters = self.__validate_named_args(
            parameter_group, named_args)
        # Make sure that all required parameters have been specified.
        bound_param_names = (
            set(p.name for (p, _) in positional_parameters) |
            set(name for (_, (name, _)) in named_parameters.items())
        )
        for param in parameter_group.values():
            if (not param.is_optional and
                    param.default_value is None and
                    param.name not in bound_param_names):
                raise MissingParameterError("Mandatory parameter '%s' is "
                                            "missing" % param.name)
        return (positional_parameters, named_parameters)

    @property
    def positional_parameters(self):
        # Return a list of parameters bound by position to their values.

        # The returned list consists of (`Parameter`, value) tuples.
        return list(self.__positional_parameters)

    @property
    def named_parameters(self):
        # dict: Map of parameters bound by name to their values.
        return self.__named_parameters.copy()

    @staticmethod
    def casefold(named_args):
        # Check that all argument names in are unique up to case.

        # Parameters
        # named_args : Mapping
            # A mapping of named parameters to values.

        # Returns
        # dict
            # A case-normalized (lowercase) copy of `named_args`.
        case_normalized_args = {}
        for k, v in named_args.items():
            normalized_k = k.lower()
            if normalized_k in case_normalized_args:
                raise ValueError(f"Parameter '{normalized_k}' passed more "
                                 "than once with different case")
            else:
                case_normalized_args[normalized_k] = v
        return case_normalized_args
