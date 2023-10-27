# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

"""
This module implements a simple representation of the BQL metadata that is
relevant for the Python client. The goal of this representation is to be
easily serializable to various formats, such as JSON or a SQLite database.

We use this format as the "canonical" format in which metadata is converted
between various formats. The Loader classes, such as `JsonMetadataLoader`
or `SqliteMetadataLoader`, support serialization and de-serialization to and
from this format.

Note that this is not the format that is used for actual consumption of the
metadata. For consumption, the metadata is typically stored and accessed in
a more structured way for quick lookup of metadata items by name, mnemonic,
or full text search.
"""


from abc import ABCMeta, abstractmethod
import six
import itertools
import logging
from .version_util import VersionInfo
from .literals import DataTypes

_logger = logging.getLogger(__name__)

_ALWAYS_UPDATE_METADATA = False


def _make_metadata_request():
    return {'option': 'ALL', 'visibility': 'INTERNAL'}


class SerializedMetadataAvailability(object):
    _UNRECOGNIZED = 0
    PRE_RELEASE = 1
    DEPRECATED = 2
    RELEASED = 3

    def from_string(avail_str):
        if avail_str == "PRE_RELEASE":
            availability = SerializedMetadataAvailability.PRE_RELEASE
        elif avail_str == "DEPRECATED":
            availability = SerializedMetadataAvailability.DEPRECATED
        elif avail_str == "RELEASED":
            availability = SerializedMetadataAvailability.RELEASED
        else:
            availability = SerializedMetadataAvailability._UNRECOGNIZED
        return availability

    def validate(availability):
        return availability in [
            SerializedMetadataAvailability._UNRECOGNIZED,
            SerializedMetadataAvailability.PRE_RELEASE,
            SerializedMetadataAvailability.DEPRECATED,
            SerializedMetadataAvailability.RELEASED
        ]


class SerializedMetadataNamespace(object):
    def __init__(self, id, name, full_path, parent_namespace):
        self.id = id
        self.name = name
        self.full_path = full_path
        self.parent_namespace = parent_namespace

    def __eq__(self, other):
        return (isinstance(other, SerializedMetadataNamespace) and
                self.id == other.id and
                self.name == other.name and
                self.full_path == other.full_path and
                self.parent_namespace == other.parent_namespace)


class SerializedMetadataOperator(object):
    INFIX = 0
    PREFIX = 1
    POSTFIX = 2

    def __init__(self, operator, function, location):
        self.operator = operator  # operator string
        self.function = function  # function ID
        self.location = location  # one of INFIX, PREFIX, POSTFIX

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataOperator) and
            self.operator == other.operator and
            self.function == other.function and
            self.location == other.location
        )
        return equals


class SerializedMetadataParameter(object):
    # The current notion of a parameter type used to be a simple Boolean
    # is_list property.  The values of the parameter types below take this
    # into account, acknowledging that when is_list was around we really
    # only distinguished between single values and list values.
    TYPE_SINGLE = 0
    TYPE_LIST = 1
    # Some parameters can take either a single value or a list of values.
    TYPE_LIST_SINGLE = 2
    # This corresponds to the BQL range() keyword.
    TYPE_RANGE = 3
    # Either a single value or range().
    TYPE_SINGLE_RANGE = 4
    # Either a list or a range().
    TYPE_LIST_RANGE = 5
    TYPE_VARARG = 6
    # Either a single value, or list, or range().
    TYPE_SINGLE_LIST_RANGE = 7

    def __init__(self, id, name, data_type, description, default_value,
                 is_optional, param_type, allows_multiple, enumerants,
                 enum_name, aliases):
        assert enumerants is None or data_type == DataTypes.ENUM[1]
        assert param_type in [
            SerializedMetadataParameter.TYPE_SINGLE,
            SerializedMetadataParameter.TYPE_LIST,
            SerializedMetadataParameter.TYPE_LIST_SINGLE,
            SerializedMetadataParameter.TYPE_RANGE,
            SerializedMetadataParameter.TYPE_SINGLE_RANGE,
            SerializedMetadataParameter.TYPE_LIST_RANGE,
            SerializedMetadataParameter.TYPE_VARARG,
            SerializedMetadataParameter.TYPE_SINGLE_LIST_RANGE,
        ]
        self.id = id
        self.name = name
        self.data_type = data_type
        self.description = description
        self.default_value = default_value
        self.is_optional = is_optional
        self.param_type = param_type
        self.allows_multiple = allows_multiple
        self.enumerants = enumerants
        self.enum_name = enum_name
        self.aliases = aliases or []

    def __eq__(self, other):

        if not isinstance(other, SerializedMetadataParameter):
            return False

        if ((self.enumerants is None and other.enumerants is not None) or
                (self.enumerants is not None and other.enumerants is None)):
            return False

        if (self.enumerants is not None and other.enumerants is not None and
                sorted(self.enumerants) != sorted(other.enumerants)):
            return False

        equals = (
            self.id == other.id and
            self.name == other.name and
            self.data_type == other.data_type and
            self.description == other.description and
            self.default_value == other.default_value and
            self.is_optional == other.is_optional and
            self.param_type == other.param_type and
            self.allows_multiple == other.allows_multiple and
            self.enum_name == other.enum_name and
            sorted(self.aliases) == sorted(other.aliases)
        )
        return equals


class SerializedMetadataColumn(object):
    def __init__(self, id, name, data_type, description, is_default):
        self.id = id
        self.name = name
        self.data_type = data_type
        self.description = description
        self.is_default = is_default

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataColumn) and
            self.id == other.id and
            self.name == other.name and
            self.data_type == other.data_type and
            self.description == other.description and
            self.is_default == other.is_default
        )
        return equals


class SerializedMetadataParameterGroup(object):
    def __init__(self, id, name, description, parameters):
        self.id = id
        self.name = name
        self.description = description
        self.parameters = list(parameters)

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataParameterGroup) and
            self.id == other.id and
            self.name == other.name and
            self.description == other.description and
            sorted(self.parameters) == sorted(other.parameters)
        )
        return equals


class SerializedMetadataColumnGroup(object):
    def __init__(self, id, name, description, columns):
        self.id = id
        self.name = name
        self.description = description
        self.columns = columns

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataColumnGroup) and
            self.id == other.id and
            self.name == other.name and
            self.description == other.description and
            sorted(self.columns) == sorted(other.columns)
        )
        return equals


class SerializedMetadataDataItem(object):
    def __init__(self, id, name, aliases, namespace, mnemonic,
                 default_parameter_group, is_bulk, is_hidden, is_periodic,
                 description, parameters, availability):
        assert SerializedMetadataAvailability.validate(availability)
        self.id = id
        self.name = name
        self.aliases = aliases
        self.namespace = namespace
        self.mnemonic = mnemonic
        self.default_parameter_group = default_parameter_group
        self.is_bulk = is_bulk
        self.is_hidden = is_hidden
        self.is_periodic = is_periodic
        self.description = description
        self.parameter_groups = list(parameters)
        self.availability = availability

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataDataItem) and
            self.id == other.id and
            self.name == other.name and
            sorted(self.aliases) == sorted(other.aliases) and
            self.namespace == other.namespace and
            self.mnemonic == other.mnemonic and
            self.default_parameter_group == other.default_parameter_group and
            self.is_bulk == other.is_bulk and
            self.is_hidden == other.is_hidden and
            self.is_periodic == other.is_periodic and
            self.description == other.description and
            sorted(self.parameter_groups) == sorted(other.parameter_groups) and
            self.availability == other.availability
        )
        return equals


class SerializedMetadataFunction(object):
    def __init__(self, id, name, aliases, namespace, is_hidden, is_aggregating,
                 mnemonic, description, return_type, macro_expression,
                 macro_processor, parameters, availability):
        assert SerializedMetadataAvailability.validate(availability)
        self.id = id
        self.name = name
        self.aliases = aliases
        self.namespace = namespace
        self.is_hidden = is_hidden
        self.is_aggregating = is_aggregating
        self.mnemonic = mnemonic
        self.description = description
        self.return_type = return_type
        self.macro_expression = macro_expression
        self.macro_processor = macro_processor
        self.parameter_groups = list(parameters)
        self.availability = availability

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataFunction) and
            self.id == other.id and
            self.name == other.name and
            sorted(self.aliases) == sorted(other.aliases) and
            self.namespace == other.namespace and
            self.is_hidden == other.is_hidden and
            self.is_aggregating == other.is_aggregating and
            self.mnemonic == other.mnemonic and
            self.description == other.description and
            self.return_type == other.return_type and
            self.macro_expression == other.macro_expression and
            self.macro_processor == other.macro_processor and
            sorted(self.parameter_groups) == sorted(other.parameter_groups) and
            self.availability == other.availability
        )
        return equals


class SerializedMetadataUniverseHandler(object):
    def __init__(self, id, keyword, description, parameters, availability):
        assert SerializedMetadataAvailability.validate(availability)
        self.id = id
        self.keyword = keyword
        self.description = description
        self.parameter_groups = list(parameters)
        self.availability = availability

    def __eq__(self, other):
        equals = (
            isinstance(other, SerializedMetadataUniverseHandler) and
            self.id == other.id and
            self.keyword == other.keyword and
            self.description == other.description and
            sorted(self.parameter_groups) == sorted(other.parameter_groups) and
            self.availability == other.availability
        )
        return equals


class SerializedMetadata:
    def __init__(self, version, function_namespaces, data_item_namespaces,
                 parameters, columns, parameter_groups, column_groups,
                 data_items, functions, universe_handlers, operators,
                 metadata_build_version=None):
        self.version = version
        self.metadata_build_version = metadata_build_version
        self.function_namespaces = list(function_namespaces)
        self.data_item_namespaces = list(data_item_namespaces)
        self.parameters = list(parameters)
        self.columns = list(columns)
        self.parameter_groups = list(parameter_groups)
        self.column_groups = list(column_groups)
        self.data_items = list(data_items)
        self.functions = list(functions)
        self.universe_handlers = list(universe_handlers)
        self.operators = list(operators)

    def __eq__(self, other):

        def compare_lists(first, second):
            return (sorted(first, key=lambda x: x.id) ==
                    sorted(second, key=lambda x: x.id))

        def compare_op_lists(first, second):
            return (sorted(first, key=lambda x: x.operator) ==
                    sorted(second, key=lambda x: x.operator))

        equals = (
            isinstance(other, SerializedMetadata) and
            self.version == other.version and
            compare_lists(self.function_namespaces,
                          other.function_namespaces) and
            compare_lists(self.data_item_namespaces,
                          other.data_item_namespaces) and
            compare_lists(self.parameters, other.parameters) and
            compare_lists(self.columns, other.columns) and
            compare_lists(self.parameter_groups, other.parameter_groups) and
            compare_lists(self.column_groups, other.column_groups) and
            compare_lists(self.data_items, other.data_items) and
            compare_lists(self.functions, other.functions) and
            compare_lists(self.universe_handlers, other.universe_handlers) and
            compare_op_lists(self.operators, other.operators)
        )
        return equals


@six.add_metaclass(ABCMeta)
class MetadataDeserializer():
    """An abstract interface to de-serialize metadata."""
    class NotModified(Exception):
        pass

    def load_metadata(self,
                      prev_version=None,
                      prev_metadata_build_version=None):

        if not self.__should_download_metadata(
                prev_version, prev_metadata_build_version):
            raise MetadataDeserializer.NotModified()

        return self._load_metadata(prev_version=prev_version)

    @abstractmethod
    def _load_metadata(self, prev_version):
        """Return a `SerializedMetadata` instance with the loaded metadata."""
        # prev_version is no longer used.  Checks against the previous version
        # are now done in load_metadata().
        raise NotImplementedError

    @abstractmethod
    def _get_version(self):
        """ Return a VersionInfo namedtuple"""
        raise NotImplementedError

    def __should_download_metadata(self,
                                   prev_version,
                                   prev_metadata_build_version=None):
        if _ALWAYS_UPDATE_METADATA:
            return True

        version_info = self._get_version()
        available_metadata_build_version = version_info.metadata_build_version

        _logger.info("Available metadata build version on the backend: %s",
                     available_metadata_build_version)

        if (prev_metadata_build_version is None or
                available_metadata_build_version is None):
            return True

        return available_metadata_build_version != prev_metadata_build_version


@six.add_metaclass(ABCMeta)
class MetadataSerializer():
    """An abstract interface to serialize metadata."""
    @abstractmethod
    def store_metadata(self, metadata):
        """Store a `SerializedMetadata` instance."""
        raise NotImplementedError


def merge_serialized_metadata(*serialized_metadata):
    """Merge multiple :class:`SerializedMetadata` instances.

    If a BQL object (parameter, parameter group, column,
    column group, enumeration, function, data item, or
    universe handler) has the same ID in two or more
    instances, it is assumed that all its properties are
    the same, but no explicit check is made.
    """
    def uniquify(objects):
        val = {obj.id: obj for obj in itertools.chain(*objects)}
        return val.values()

    def uniquify_operators(operators):
        val = {(obj.operator, obj.location): obj
               for obj in itertools.chain(*operators)}
        return val.values()

    versions = set(x.version for x in serialized_metadata
                   if x.version is not None)
    if len(versions) > 1:
        raise RuntimeError(
            f'Cannot merge metadata of different versions. '
            f'Encountered: {versions}')

    if versions:
        version = next(iter(versions))
    else:
        version = None

    function_namespaces = uniquify(x.function_namespaces
                                   for x in serialized_metadata)
    data_item_namespaces = uniquify(x.data_item_namespaces
                                    for x in serialized_metadata)
    parameters = uniquify(x.parameters for x in serialized_metadata)
    columns = uniquify(x.columns for x in serialized_metadata)
    pgroups = uniquify(x.parameter_groups for x in serialized_metadata)
    cgroups = uniquify(x.column_groups for x in serialized_metadata)
    data_items = uniquify(x.data_items for x in serialized_metadata)
    functions = uniquify(x.functions for x in serialized_metadata)
    universe_handlers = uniquify(x.universe_handlers
                                 for x in serialized_metadata)
    operators = uniquify_operators(x.operators for x in serialized_metadata)

    return SerializedMetadata(version, function_namespaces,
                              data_item_namespaces, parameters, columns,
                              pgroups, cgroups, data_items, functions,
                              universe_handlers, operators)
