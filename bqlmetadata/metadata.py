# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import copy
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Mapping
import six

from bqmonitor.permissions import is_enabled_for_bqmonitor
from .exceptions import MetadataError
from .literals import (DataTypes,
                       LiteralTypeError,
                       check_type_conversion)
from .metadata_serializer import (
    SerializedMetadataAvailability as MetadataAvailability,
    SerializedMetadataOperator,
    SerializedMetadataParameter
)
from .range import Range



_logger = logging.getLogger(__name__)


class _FederatedMetadata(ABC):
    data_types = {}
    literals = {}
    next_id = -1

    # Extract the data types and literals that will be used
    for __data_type_name in ('string', 'date', 'int'):
        __data_type = getattr(DataTypes, __data_type_name.upper())
        data_types[__data_type_name] = __data_type
        literals[__data_type_name] = __data_type[2]()

    def __init__(self, metadata_reader):
        self.metadata_reader = metadata_reader
        self.dates_param = self.create_parameter(
            name="DATES",
            default=None,
            description="As of date or date range",
            data_type="date",
            is_optional=True,
            param_type=SerializedMetadataParameter.TYPE_SINGLE_RANGE,
        )
        self.fill_param = self.create_parameter(
            name="FILL",
            default="na",
            description="Backfill Protocol",
            data_type="string",
            is_optional=True,
            param_type=SerializedMetadataParameter.TYPE_SINGLE
        )

    @classmethod
    def allocate_id(cls):
        id_ = cls.next_id
        # We use increasingly negative numbers as IDs for the metadata objects.
        # Note that we are updating the class attribute on the base class so
        # there is only a single place to keep track of the next ID.
        _FederatedMetadata.next_id -= 1
        return id_

    @classmethod
    def create_parameter(cls, name, data_type, default, description,
                         is_optional, param_type):
        return Parameter(
            id_=cls.allocate_id(),
            name=name,
            data_type=cls.data_types[data_type],
            literal=cls.literals[data_type],
            default_value=default,
            description=description,
            is_optional=is_optional,
            param_type=param_type,
            aliases=[],
            allows_multiple=False
        )

    @classmethod
    def create_parameter_group(cls, name, *params):
        return ParameterGroup(cls.allocate_id(), name, *params)

    @abstractmethod
    def create_metadata(self):
        pass

    def _create_metadata(self, name, description, output_cols,
                         parameter_groups):
        return MetadataItem.create_data_item(
            id_=self.allocate_id(),
            name=name,
            mnemonic=None,
            description=description,
            parameter_groups=parameter_groups,
            output_cols=output_cols,
            metadata_reader=self.metadata_reader
        )


class _CDEMetadata(_FederatedMetadata):
    def create_metadata(self):
        single_param = SerializedMetadataParameter.TYPE_SINGLE
        start_param = self.create_parameter(
            "START",
            default=None,
            description="Start date",
            data_type="date",
            is_optional=True,
            param_type=single_param,
        )
        end_param = self.create_parameter(
            name="END",
            default=None,
            description="End date",
            data_type="date",
            is_optional=True,
            param_type=single_param,
        )
        frq_param = self.create_parameter(
            name="FRQ",
            # If we make FRQ an ENUM instead of a STRING, then we'll generate
            # query strings with parameter values like FRQ.D.  BQL doesn't
            # accept these prefixed values yet for the CDE fields, so we need
            # to keep FRQ as a generic STRING.  We lose some client-side
            # validation, but that's OK--we'll eventually have real metadata
            # for the CDEs.
            default="D",
            description="Frequency",
            data_type="string",
            is_optional=True,
            param_type=single_param,
        )
        pg1 = self.create_parameter_group("CDE_PGROUP_1", start_param,
                                          end_param, frq_param, self.fill_param)
        pg2 = self.create_parameter_group("CDE_PGROUP_2", self.dates_param,
                                          self.fill_param)
        return self._create_metadata(
            name="_CDE",
            description="Custom data field",
            output_cols=["ID", "DATE"],
            parameter_groups=[pg1, pg2]
        )


class _ModMgmtMetadata(_FederatedMetadata):
    def create_metadata(self):
        single_param = SerializedMetadataParameter.TYPE_SINGLE
        model_param = self.create_parameter(
            "MODEL",
            default=None,
            description="Model Name",
            data_type="string",
            is_optional=False,
            param_type=single_param,
        )
        version_param = self.create_parameter(
            "VERSION",
            default=None,
            description="Model Version",
            data_type="int",
            is_optional=True,
            param_type=single_param,
        )
        run_param = self.create_parameter(
            "RUN",
            default=None,
            description="Model Run Number",
            data_type="int",
            is_optional=True,
            param_type=single_param,
        )
        pg1 = self.create_parameter_group(
            "MODEL_PGROUP_1", model_param, version_param, run_param,
            self.dates_param, self.fill_param
        )
        return self._create_metadata(
            name="_MODELMGMT",
            description="Model Management Field",
            output_cols=["ID", "DATE"],
            parameter_groups=[pg1]
        )


class MetadataItem(object):
    """A top-level metadata object.

    This is for either a BQL function, data item, or universe handler.
    """
    _DATA_ITEM = 1
    _FUNCTION = 2
    _UNIVERSE_HANDLER = 3

    @staticmethod
    def validate(id_,
                 type_,
                 name,
                 mnemonic,
                 description,
                 parameter_groups,
                 operator_symbol,
                 operator_location,
                 availability=MetadataAvailability.RELEASED):

        if id_ is None:
            return False, "Metadata item's id is None"

        if name is None:
            return False, "Metadata item's name is None"

        if type_ not in (MetadataItem._DATA_ITEM,
                         MetadataItem._FUNCTION,
                         MetadataItem._UNIVERSE_HANDLER):
            return False, f"Invalid metadata type: {type_} for item {name}"

        if parameter_groups is None:
            return False, f"parameter_groups is None for item {name}"

        if len(parameter_groups) == 0:
            return False, f"Empty parameter_groups for item {name}"

        # Data items don't have infix operators.
        if type_ != MetadataItem._FUNCTION and operator_symbol is not None:
            return (
                False,
                f"Data item cannot have infix operator, item: {name}")

        if not MetadataAvailability.validate(availability):
            return (
                False,
                f"Invalid availability type {availability} for item {name}")

        return True, ""

    def __init__(self, id_, type_, name, mnemonic, description,
                 parameter_groups, operator_symbol, operator_location,
                 output_cols, metadata_reader,
                 availability=MetadataAvailability.RELEASED):
        """Use create_data_item() or create_function() instead."""

        ok, reason = MetadataItem.validate(
            id_, type_, name, mnemonic, description,
            parameter_groups, operator_symbol, operator_location, availability)

        if not ok:
            raise MetadataError(f"MetadataError: {reason}")

        self._id = id_
        self._type = type_
        self._name = name
        self._mnemonic = mnemonic
        self._description = description
        self._parameter_groups = list(parameter_groups)
        self._operator_symbol = operator_symbol
        self._operator_location = operator_location
        self._output_cols = output_cols
        self._metadata_reader = metadata_reader
        self._availability = availability

    def __repr__(self):
        type_ = {
            MetadataItem._DATA_ITEM: "DATA",
            MetadataItem._FUNCTION: "FUNCTION",
            MetadataItem._UNIVERSE_HANDLER: "UNIVERSE_HANDLER"
        }
        return "<MetadataItem(%s,id=%s,name=%s,mnemonic=%s,pgroups=%s)>" % (
            type_[self._type], self._id, self._name, self._mnemonic,
            self._parameter_groups)

    def __hash__(self):
        return hash((self._id, self._type))

    def __eq__(self, other):
        return (isinstance(other, MetadataItem) and
                self._id == other._id and
                self._type == other._type)

    def __ne__(self, other):
        return not (self == other)

    @staticmethod
    def create_data_item(id_, name, mnemonic, description, parameter_groups,
                         output_cols, metadata_reader,
                         availability=MetadataAvailability.RELEASED):
        """Create a MetadataItem for a data item."""
        operator_symbol = None
        operator_location = None
        return MetadataItem(id_,
                            MetadataItem._DATA_ITEM,
                            name,
                            mnemonic,
                            description,
                            parameter_groups,
                            operator_symbol,
                            operator_location,
                            list(set(output_cols+["VALUE"])),
                            metadata_reader,
                            availability)

    @staticmethod
    def create_function(id_, name, mnemonic, description, parameter_groups,
                        operator_symbol, operator_location, metadata_reader,
                        availability=MetadataAvailability.RELEASED):
        """Create a MetadataItem for a function."""
        return MetadataItem(id_,
                            MetadataItem._FUNCTION,
                            name,
                            mnemonic,
                            description,
                            parameter_groups,
                            operator_symbol,
                            operator_location,
                            None,
                            metadata_reader,
                            availability)

    @staticmethod
    def create_universe_handler(
            id_, name, mnemonic, description, parameter_groups, metadata_reader,
            availability=MetadataAvailability.RELEASED):
        """Create a MetadataItem for a universe handler."""
        operator_symbol = None
        operator_location = None
        return MetadataItem(id_, MetadataItem._UNIVERSE_HANDLER, name,
                            mnemonic, description, parameter_groups,
                            operator_symbol, operator_location,
                            None, metadata_reader, availability)

    @property
    def name(self):
        return self._name

    @property
    def mnemonic(self):
        return self._mnemonic

    @property
    def parameter_groups(self):
        return self._parameter_groups

    @property
    def description(self):
        return self._description

    @property
    def availability(self):
        return self._availability

    @property
    def reader(self):
        """Return the MetadataReader that created this MetadataItem.

        If this object was created outside a MetadataReader, then return None.
        Because MetadataReaders are typically the way MetadataItems are
        created, knowing the MetadataReader allows you to create other
        MetadataItems in a way similar to this MetadataItem.  For example:

            px_last().avg()

        Presumably you'd want the metadata for the avg() function to come from
        the same source as the metadata for the px_last() data item.
        """
        return self._metadata_reader

    @property
    def infix_operator(self):
        """Return the infix operator reserved for this function.

        It's this infix operator that should be used to represent the
        function in a query request string (e.g., the infix "+" operator
        instead of the "ADD" function name).  If this function doesn't
        have an operator, or this MetadataItem is for a data item, then
        return ``None``.
        """
        if self._operator_location == SerializedMetadataOperator.INFIX:
            return self._operator_symbol
        return None

    @property
    def prefix_operator(self):
        """Return the prefix operator reserved for this function.

        It's this prefix operator that should be used to represent the
        function in a query request string (e.g., the prefix "-" operator
        instead of the "NEGATION" function name).  If this function doesn't
        have an operator, or this MetadataItem is for a data item, then
        return ``None``.
        """
        if self._operator_location == SerializedMetadataOperator.PREFIX:
            return self._operator_symbol

        return None

    @property
    def return_type(self):
        if self._type == MetadataItem._DATA_ITEM:
            return DataTypes.ITEM
        elif self._type == MetadataItem._UNIVERSE_HANDLER:
            return DataTypes.UNIVERSE
        elif self._type == MetadataItem._FUNCTION:
            return DataTypes.DERIVED

        # The type is checked during construction, so
        # we should not be here
        assert False, f"Invalid MetadataItem type: {self._type}"

    @staticmethod
    def _create_cde_metadata(metadata_reader):
        return _CDEMetadata(metadata_reader).create_metadata()

    @staticmethod
    def _create_model_mgmt_metadata(metadata_reader):
        return _ModMgmtMetadata(metadata_reader).create_metadata()

    def _set_metadata_reader(self, metadata_reader):
        # Make it look like this MetadataItem came from another MetadataReader.
        # Note that this does an in-place change of the metadata reader.  If
        # you want to retain the original MetadataItem, then call
        # _copy_with_metadata_reader() instead.
        self._metadata_reader = metadata_reader

    def _copy_with_metadata_reader(self, metadata_reader):
        # Create a new MetadataItem that is a copy of "self" but with a new
        # metadata_reader.
        metadata_item = copy.copy(self)
        metadata_item._set_metadata_reader(metadata_reader)
        return metadata_item


class ParameterGroup(Mapping):
    def __init__(self, id_, name, *parameters):
        """Create a new parameter group.

        parameters is a sequence of :class:`Parameter` instances."""

        self._id = id_
        self._name = name
        self._parameters = OrderedDict((param.name, param)
                                       for param in parameters)

        # This check makes sure that all parameters have a different name.
        # If that is not the case, the ordered dict would swallow some
        # parameters. See also BQUN-469.
        #
        # In this case we raise an exception and drop this parameter group.
        # We will continue with the construction of the rest of the
        # metadata, but will generate an alarm and raise a ticket.
        # TODO: Link to splunk rule for this error.

        if len(self._parameters) != len(parameters):
            raise MetadataError(
                "MetadataError: Duplicate bql parameters received "
                f"for parameter group: {name} parameters: {parameters}")

        self._parameters_by_alias = {}
        for param in parameters:
            for alias in param.aliases:

                if alias in self._parameters_by_alias:
                    # Here we also log error, but continue
                    # with the construction of the parameter group
                    # without the duplicate alias.
                    _logger.error(
                        "MetadataError: '%s' is used as a parameter alias "
                        "more than once in parameter group %s",
                        alias, self._name, extra={'suppress': True})
                    continue

                self._parameters_by_alias[alias] = param

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return "<ParameterGroup(id=%s,[%s])>" % (
            self._id, ",".join(map(str, self._parameters.values())))

    def __len__(self):
        return len(self._parameters)

    def __getitem__(self, key):
        return self._parameters[key]

    def __iter__(self):
        return iter(self._parameters)

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        # Define equality in terms of the unique parameter group ID
        return isinstance(other, ParameterGroup) and self._id == other._id

    def __ne__(self, other):
        return not (self == other)

    def get_parameter(self, name_or_alias):
        try:
            # Note that in the unlikely event that there's a parameter with
            # an alias that is the same as another parameter's name, we're
            # giving priority to the name.
            return self._parameters[name_or_alias]
        except KeyError:
            return self._parameters_by_alias.get(name_or_alias)


class Parameter(object):
    def __init__(self, id_, name, data_type, literal, default_value,
                 description, is_optional, param_type, aliases,
                 allows_multiple=False):

        if param_type not in [
            SerializedMetadataParameter.TYPE_SINGLE,
            SerializedMetadataParameter.TYPE_LIST,
            SerializedMetadataParameter.TYPE_LIST_SINGLE,
            SerializedMetadataParameter.TYPE_RANGE,
            SerializedMetadataParameter.TYPE_SINGLE_RANGE,
            SerializedMetadataParameter.TYPE_LIST_RANGE,
            SerializedMetadataParameter.TYPE_VARARG,
            SerializedMetadataParameter.TYPE_SINGLE_LIST_RANGE,
        ]:
            raise MetadataError(
                f"MetadataError: Invalid parameter type: {param_type} "
                f"for parameter: {name}")

        if aliases is None:
            raise MetadataError(
                "MetadataError: Expected aliases to not be none "
                f"for parameter: {name}")

        self._id = id_
        self._name = name.lower()
        self._data_type = data_type
        # Note that literal can be None when there is no literal representation
        # for this parameter in BQL syntax, e.g. for parameters of type ITEM.
        self._literal = literal
        if self._literal is not None and default_value is not None:
            try:
                self._default_value = self._literal.validate(default_value)
            except LiteralTypeError as e:
                raise MetadataError(f"MetadataError: {e.args[0]}")
        elif data_type == DataTypes.ITEM:
            # ENG2BQNTFL-749: For now ignore default values for ITEMs.  Because
            # default values need to be validated against the ITEM parameter
            # type, we'd need to parse the string default value in the metadata
            # into a BqlItem, whenever the parameter's default value needs to
            # be accessed.  That in turn would need to be done in at the pybql
            # level; we'd probably always store the string default value here
            # in the metadata Parameter object.
            self._default_value = None
        else:
            self._default_value = default_value
        self._description = description
        self._is_optional = is_optional
        self._param_type = param_type
        self._aliases = [a.lower() for a in aliases]
        self._allows_multiple = allows_multiple

    def __repr__(self):
        return "<Parameter('%s',id=%s)>" % (self._name, self._id)

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        # Define equality in terms of the unique parameter ID
        return isinstance(other, Parameter) and self._id == other._id

    def __ne__(self, other):
        return not (self == other)

    @property
    def name(self):
        return self._name

    @property
    def default_value(self):
        return self._default_value

    @property
    def description(self):
        return self._description

    @property
    def is_optional(self):
        return self._is_optional

    @property
    def data_type(self):
        return self._data_type

    @property
    def literal(self):
        return self._literal

    @property
    def aliases(self):
        return self._aliases

    @property
    def allows_multiple(self):
        return self._allows_multiple

    def is_vararg(self):
        return self._param_type == SerializedMetadataParameter.TYPE_VARARG

    def validate(self, val):
        # TODO(kwhisnant): Change LiteralTypeErrors below to different types
        # of exceptions?
        if Parameter.__is_range(val):
            # Does this parameter accept a range?
            if self.__can_be_range():
                start = self.__validate_single_value(val.start)
                end = self.__validate_single_value(val.end)
                validated_value = Range(start, end)
            else:
                raise LiteralTypeError(
                    f'Parameter "{self._name}" cannot be a range')
        elif Parameter.__is_range_function(val):
            if self.__can_be_range():
                # We're liberally accepting the range() function even if it's
                # for a range that doesn't agree with this parameter's type
                # (e.g., if this is a "dates" parameter that expects a range
                # of dates, we'll accept a range of ints too since this is
                # is a valid instantiation of range()).  We'll let the backend
                # raise an error in this case
                validated_value = val
            else:
                raise LiteralTypeError(
                    f'Parameter "{self._name}" cannot be a range')
        elif self.__must_be_range():
            raise LiteralTypeError(
                f'Need range parameter "{self._name}", not {str(type(val))}')
        elif Parameter.__is_list(val):
            # Does this parameter accept lists?
            if self.__can_be_list():
                # The list is validated by validating each item separately.
                validated_value = [self.__validate_single_value(v)
                                   for v in val]
            else:
                raise LiteralTypeError(
                    f'Parameter "{self._name}" cannot be a list')
        elif self.__must_be_list():
            raise LiteralTypeError('Need an iterable type '
                                   f'for list parameter "{self._name}", '
                                   f'not {str(type(val))}')
        else:
            # This must be a single value.
            validated_value = self.__validate_single_value(val)
        return validated_value

    def format_for_query_string(self, val):
        return self.__format(val, target="query")

    def format_for_docstring(self, val):
        return self.__format(val, target="doc")

    @staticmethod
    def __is_list(val):
        """
        Return True if val can be accepted by a Parameter that takes a list.
        """
        # We're treating anything iterable as a list, except for strings.
        if isinstance(val, six.string_types):
            is_list = False
        else:
            try:
                iter(val)
            except TypeError:
                is_list = False
            else:
                is_list = True
        return is_list

    @staticmethod
    def __is_range(val):
        return isinstance(val, Range)

    @staticmethod
    def __is_range_function(val):
        try:
            metadata = val._metadata
        except AttributeError:
            # This doesn't look like a BqlItem, so it can't be the range()
            # function.
            return False
        else:
            for m in metadata:
                if m._type == MetadataItem._FUNCTION and m._name == "RANGE":
                    return True
            return False

    def __can_be_list(self):
        return self._param_type in (
            SerializedMetadataParameter.TYPE_LIST,
            SerializedMetadataParameter.TYPE_LIST_SINGLE,
            SerializedMetadataParameter.TYPE_LIST_RANGE,
            SerializedMetadataParameter.TYPE_SINGLE_LIST_RANGE,
        )

    def __must_be_list(self):
        return (self._param_type == SerializedMetadataParameter.TYPE_LIST)

    def __can_be_range(self):
        return self._param_type in (
            SerializedMetadataParameter.TYPE_RANGE,
            SerializedMetadataParameter.TYPE_SINGLE_RANGE,
            SerializedMetadataParameter.TYPE_LIST_RANGE,
            SerializedMetadataParameter.TYPE_SINGLE_LIST_RANGE
        )

    def __must_be_range(self):
        return (self._param_type == SerializedMetadataParameter.TYPE_RANGE)

    def __validate_single_value(self, val):
        # BqlItems have return values that we need to check against.
        try:
            return_type = val.return_type
        except AttributeError:
            # This doesn't look like a BqlItem.
            if self._literal is None:
                raise LiteralTypeError('Cannot specify a literal value for '
                                       f'parameter "{self._name}" '
                                       f'(type {self._data_type[0]}). Please '
                                       'use a function or data item instead.')
            return self._literal.validate(val)
        else:
            check_type_conversion(return_type, self._data_type)
            return val

    def __format(self, val, target):
        assert target in ["query", "doc"]
        if Parameter.__is_range(val):
            result = (f"range({self.__format(val.start, target)},"
                      f"{self.__format(val.end, target)})")
        elif Parameter.__is_list(val):
            param_list = (self.__format(v, target) for v in val)
            # for bqmonitor to be able to consistently hash bql requests,
            # we need our param list elements to be deterministic in ordering
            # hence we sort the elements in the list on formatting
            if is_enabled_for_bqmonitor():
                param_list = sorted(param_list)
            result = f'[{", ".join(param_list)}]'
        else:
            # val is a single item or literal.
            try:
                # Note that for docstrings we format a BqlItem in the same way
                # we do for a query string.
                bql_item_formatter = val.format_for_query_string
            except AttributeError:
                # This doesn't look like a BqlItem.
                if target == "query":
                    result = self._literal.to_bql(val)
                else:
                    result = self._literal.format_for_docstring(val)
            else:
                result = bql_item_formatter()
        return result
