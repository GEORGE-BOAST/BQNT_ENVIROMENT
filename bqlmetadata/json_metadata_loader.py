#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from bqutil import ScopedTracer

import functools
import json


from .metadata_serializer import (SerializedMetadata,
                                  SerializedMetadataAvailability,
                                  SerializedMetadataColumn,
                                  SerializedMetadataColumnGroup,
                                  SerializedMetadataDataItem,
                                  SerializedMetadataFunction,
                                  SerializedMetadataNamespace,
                                  SerializedMetadataOperator,
                                  SerializedMetadataParameter,
                                  SerializedMetadataParameterGroup,
                                  SerializedMetadataUniverseHandler)
from .literals import DataTypes, ParameterTypes, ColumnTypes, ReturnTypes

import logging

_logger = logging.getLogger(__name__)


class JsonMetadataLoader(object):
    """A helper class for loading official JSON metadata."""

    _SUPPORTED_SCHEMA_MAJOR_VERSION = 1

    def __init__(self,
                 version,
                 schema_version,
                 metadata_build_version=None):
        self._version = version
        self._metadata_build_version = metadata_build_version
        self._function_namespaces = {}
        self._data_item_namespaces = {}
        self._parameters = {}
        self._parameter_groups = {}
        self._columns = {}
        self._column_groups = {}
        self._data_items = {}
        self._functions = {}
        self._universe_handlers = {}
        self._operators = []

        if (schema_version is not None and
                schema_version[0] != self._SUPPORTED_SCHEMA_MAJOR_VERSION):
            raise RuntimeError(
                f'Metadata has schema version {schema_version}, but this '
                f'version of pybql only supports major schema version '
                f'{self._SUPPORTED_SCHEMA_MAJOR_VERSION}. Consider '
                f'upgrading pybql.')

    def _load_enums(self, enums):
        enum_map = {}
        for enum in enums:
            if enum['id'] in enum_map:
                _logger.info("MetadataError: Duplicate enums: %s and %s",
                             enum,
                             enum_map[enum['id']],
                             extra={'suppress': True})
                continue

            enum_map[enum['id']] = (enum["name"],
                                    [v["value"] for v in enum["values"]])
        return enum_map

    def _import_parameter(self, param, enums):
        id = param['id']

        if id in self._parameters:
            _logger.info("MetadataError: Duplicate parameter: %s",
                         param, extra={'suppress': True})
            return

        name = param['name']
        data_type_str = param['parameterValueType']
        description = param.get('description', None)
        default_value = param.get('defaultValue', None)
        is_optional = param['optional']
        if param['parameterType'] == 'LIST':
            param_type = SerializedMetadataParameter.TYPE_LIST
        elif param['parameterType'] == 'SINGLE_LIST':
            param_type = SerializedMetadataParameter.TYPE_LIST_SINGLE
        elif param['parameterType'] == 'SINGLE':
            param_type = SerializedMetadataParameter.TYPE_SINGLE
        elif param['parameterType'] == 'SINGLE_RANGE':
            param_type = SerializedMetadataParameter.TYPE_SINGLE_RANGE
        elif param['parameterType'] == 'RANGE':
            param_type = SerializedMetadataParameter.TYPE_RANGE
        elif param['parameterType'] == 'LIST_RANGE':
            param_type = SerializedMetadataParameter.TYPE_LIST_RANGE
        elif param['parameterType'] == 'VARARG':
            param_type = SerializedMetadataParameter.TYPE_VARARG
        elif param['parameterType'] == 'SINGLE_LIST_RANGE':
            param_type = SerializedMetadataParameter.TYPE_SINGLE_LIST_RANGE
        else:
            raise ValueError(
                f"Unexpected parameterType '{param['parameterType']}' for "
                f"{param}")

        allows_multiple = False

        # validate parameter type
        data_type = ParameterTypes.type_from_name(data_type_str)

        if data_type == DataTypes.ENUM:
            enum_id = param['enumeration']['id']
            (enum_name, enumerants) = enums[enum_id]
        else:
            enumerants = None
            enum_name = None

        aliases = param.get("aliases", [])

        self._parameters[id] = SerializedMetadataParameter(
            id, name, data_type[1], description, default_value,
            is_optional, param_type, allows_multiple, enumerants,
            enum_name, aliases)

    def _import_parameters(self, params, enums):
        for param in params:
            self._import_parameter(param, enums)

    def _import_parameter_group(self, pgroup):
        id = pgroup['id']

        if id in self._parameter_groups:
            _logger.info("MetadataError: Duplicate parameter group: %s",
                         pgroup, extra={'suppress': True})
            return

        param_ids = [self._parameters[x['id']].id
                     for x in pgroup.get('parameters', [])]
        name = pgroup['name']
        description = pgroup.get('description', None)

        self._parameter_groups[id] = SerializedMetadataParameterGroup(
            id, name, description, param_ids)

    def _import_parameter_groups(self, pgroups):
        for pgroup in pgroups:
            self._import_parameter_group(pgroup)

    def _import_column(self, column):
        id = column['id']

        if id in self._columns:
            _logger.info("MetadataError: Duplicate column: %s",
                         column, extra={'suppress': True})
            return id

        name = column['name']
        data_type_str = column['columnValueType']
        description = column.get('description', None)
        is_default = column['isDefault']

        # validate column type
        column_type = ColumnTypes.type_from_name(data_type_str)

        self._columns[id] = SerializedMetadataColumn(
            id, name, column_type[1], description, is_default)

        return id

    def _import_columns(self, columns):
        for column in columns:
            self._import_column(column)

    def _import_column_group(self, cgroup):
        id = cgroup['id']

        if id in self._column_groups:
            _logger.info("MetadataError: Duplicate column group: %s",
                         cgroup, extra={'suppress': True})
            return

        column_ids = [self._columns[x['id']].id
                      for x in cgroup.get('columns', [])]
        name = cgroup['name']
        description = cgroup.get('description')

        self._column_groups[id] = SerializedMetadataColumnGroup(
            id, name, description, column_ids)

    def _import_column_groups(self, cgroups):
        for cgroup in cgroups:
            self._import_column_group(cgroup)

    def _import_function_namespace(self, namespace, parent_namespace_id):
        id = namespace['id']
        name = namespace['name']
        full_path = namespace['fullPath']

        if id in self._function_namespaces:
            return

        self._function_namespaces[id] = SerializedMetadataNamespace(
            id, name, full_path, parent_namespace_id)

    def _import_data_item_namespace(self, namespace, parent_namespace_id):
        id = namespace['id']
        name = namespace['name']
        full_path = namespace['fullPath']

        if id in self._data_item_namespaces:
            return

        self._data_item_namespaces[id] = SerializedMetadataNamespace(
            id, name, full_path, parent_namespace_id)

    def _import_function(self, func, parent_namespace_id):
        id = func['id']

        if id in self._functions:
            _logger.info("MetadataError: Duplicate function: %s",
                         func, extra={'suppress': True})
            return

        name = func['name']
        aliases = func.get('aliases', [])
        properties = func["properties"]
        is_aggregating = properties['isAggregating']
        return_type_str = properties['returnType']
        mnemonic = properties.get('mnemonic', None)
        macro_expression = properties.get('macroValue', None)
        macro_processor = properties.get('macroProcessor', None)
        description = properties.get('description', None)

        if 'functionAssociations' in func:
            # 1.10 metadata has parameter groups and visibility in
            # functionAssociations.  This does not quite fit into our view
            # of the world; for now just record EXTERNALly visibible
            # parameter groups and mark the function as hidden if none of
            # the parameter groups are visible.
            func_parameters = [
                self._parameter_groups[fa['parameterGroup']['id']].id
                for fa in func['functionAssociations']
            ]

            is_hidden = len(func_parameters) == 0
        else:
            # This is a pre-1.10 metadata
            is_hidden = (func["visibility"] == "HIDDEN")
            func_parameters = [self._parameter_groups[x['id']].id
                               for x in func['parameterGroups']]

        # validate return type
        return_type = ReturnTypes.type_from_name(return_type_str)

        availability = self._validate_availability(
            func.get("availability"), func, item_type="function")

        self._functions[id] = SerializedMetadataFunction(
            id, name, aliases, parent_namespace_id, is_hidden,
            is_aggregating, mnemonic, description, return_type[1],
            macro_expression, macro_processor, func_parameters, availability)

    def _import_functions(self, functions, parent_namespace_id):
        for namespace in functions["nameSpaces"]:
            namespace_id = namespace["id"]
            self._import_function_namespace(namespace, parent_namespace_id)
            for element in namespace["elements"]:
                self._import_function(element["function"], namespace_id)

    def _import_data_item(self, item, parent_namespace_id):
        id = item['id']

        if id in self._data_items:
            _logger.info("MetadataError: Duplicate data item: %s",
                         item, extra={'suppress': True})
            return

        name = item['name']
        aliases = item.get('aliases', [])
        # BQUN-739: Remove the obsolete is_bulk and is_periodic fields.
        is_bulk = False
        is_periodic = False
        properties = item["properties"]
        mnemonic = properties.get('mnemonic', None)
        description = properties.get('description', None)

        # bqldsvc doesn't give us a defaultParameterGroup.  We weren't using
        # it in pybql anyway, so there is no harm in setting it to None here.
        default_parameter_group_id = None

        # Register parameters and columns for this data item

        # The same parameter group/column group pair may appear more than
        # once in the matadata, with the two dataItemAssociation records
        # differing only in their mode properties.  These differences
        # are not important to us, and we only want to retain unique
        # parameter group/column group pairs.  Generate our list in the
        # order they appear in the metadata.
        param_groups = []
        unique_param_column_group_ids = set()
        for dai in item['dataItemAssociations']:
            pg_id = dai['parameterGroup']['id']
            cg_id = dai['columnGroup']['id']
            if (pg_id, cg_id) not in unique_param_column_group_ids:
                unique_param_column_group_ids.add((pg_id, cg_id))
                param_groups.append(
                    (self._parameter_groups[pg_id].id,
                     self._column_groups[cg_id].id)
                )
        # 1.10 metadata has visibility in dataItemAssociations.
        # This does not quite fit into our view of the world; for now just
        # record EXTERNALly visible parameter groups and mark the data
        # item as hidden if none of the parameter groups are visible.
        is_hidden = len(param_groups) == 0
        if 'visibility' in item:
            is_hidden = (item["visibility"] == "HIDDEN")

        availability = self._validate_availability(
            item.get("availability"), item, item_type="data item")

        # Register data item
        self._data_items[id] = SerializedMetadataDataItem(
            id, name, aliases, parent_namespace_id, mnemonic,
            default_parameter_group_id, is_bulk, is_hidden, is_periodic,
            description, param_groups, availability)

    def _import_data_items(self, data_items, parent_namespace_id):
        for namespace in data_items["nameSpaces"]:
            namespace_id = namespace["id"]
            self._import_data_item_namespace(namespace, parent_namespace_id)
            for element in namespace["elements"]:
                self._import_data_item(element["dataItem"], namespace_id)

    def _import_universe_handler(self, universe_handler):
        id_ = universe_handler['id']

        if id_ in self._universe_handlers:
            _logger.info("MetadataError: Duplicate universe handler: %s",
                         universe_handler, extra={'suppress': True})
            return

        keyword = universe_handler['properties']['keyword']

        univ_assoc = universe_handler['universeFunctionAssociations']
        pgroup_ids = [assoc['parameterGroup']['id'] for assoc in univ_assoc]

        description = None  # these don't seem to have descriptions
        availability = self._validate_availability(
            universe_handler.get("availability"),
            universe_handler,
            item_type="universe handler"
        )
        self._universe_handlers[id_] = SerializedMetadataUniverseHandler(
            id_, keyword, description, pgroup_ids, availability)

    def _import_universe_handlers(self, universe_handlers):
        for universe_handler in universe_handlers:
            self._import_universe_handler(universe_handler)

    def _import_operators(self, operators, location):
        for operator in operators:
            function_name = operator["functionName"]
            operator = operator["operatorName"]
            functions = [func for func in self._functions.values()
                         if func.name.lower() == function_name.lower()]
            if len(functions) == 0:
                raise RuntimeError(
                    f'Function "{function_name}" for operator "{operator}" '
                    'not available')
            if len(functions) > 1:
                raise RuntimeError(
                    f'More than one function "{function_name}" for operator '
                    f'"{operator}" available')

            self._operators.append(
                SerializedMetadataOperator(
                    operator, functions[0].id, location))

    def _validate_availability(self, avail_str, item, item_type):
        availability = SerializedMetadataAvailability.from_string(avail_str)
        if availability == SerializedMetadataAvailability._UNRECOGNIZED:
            # TODO(kwhisnant): Log whenever we see an item that doesn't have
            # an availability field, which indicates that the metadata isn't
            # structured the way we expect it.  But we need to be careful and
            # do this only once we transition over to the new BQL discovery
            # service for good, because the old metadata service doesn't have
            # the availability field.
            #
            # _logger.info(
            #    "MetadataError: Unrecognized availability type '%s' in %s %s",
            #    avail_str, item_type, item, extra={'suppress': True}
            # )
            pass
        return availability

    def read_chunks(self, fileobj):
        # We might get back a sequence of JSON objects back-to-back, so we
        # can't just use json.load() to parse it.
        decoder = json.JSONDecoder()
        json_string = "".join(fileobj.read().split("\n"))
        offset = 0
        chunks = []

        while offset < len(json_string):
            (chunk, new_offset) = decoder.raw_decode(json_string, idx=offset)
            assert new_offset > offset
            chunks.append(chunk["allCoreMDResponse"])
            offset = new_offset

        return chunks

    def load(self, fileobj):
        chunks = self.read_chunks(fileobj)
        return self.load_chunks(chunks)

    def load_chunks(self, chunks):
        # When going through the chunks, first load enumerations from all
        # chunks, then parameters from all chunks, etc. so that x-references
        # can be resolved.
        enums = {}
        for chunk in chunks:
            enums.update(self._load_enums(chunk.get('enumerations', [])))

        IMPORTERS = [
            (
                functools.partial(self._import_parameters, enums=enums),
                'parameters',
                []
            ),
            (
                self._import_parameter_groups,
                'parameterGroups',
                []
            ),
            (
                self._import_columns,
                'columns',
                []
            ),
            (
                self._import_column_groups,
                'columnGroups',
                []
            ),
            (
                functools.partial(self._import_functions,
                                  parent_namespace_id=None),
                'functions',
                {'nameSpaces': []}
            ),
            (
                functools.partial(self._import_data_items,
                                  parent_namespace_id=None),
                'dataItems',
                {'nameSpaces': []}
            ),
            (
                self._import_universe_handlers,
                'universeFunctions',
                []
            ),
            (
                functools.partial(self._import_operators,
                                  location=SerializedMetadataOperator.INFIX),
                'binaryOperators',
                []
            ),
            (
                functools.partial(self._import_operators,
                                  location=SerializedMetadataOperator.PREFIX),
                'unaryOperators',
                []
            )
        ]

        for importer, key, default_value in IMPORTERS:
            with ScopedTracer(name='bqlmetadata.serialization.%s' % key,
                              logger=_logger):
                for chunk in chunks:
                    importer(chunk.get(key, default_value))

    @property
    def metadata(self):
        """Access the metadata that has been read.

        Returns a :class:`SerializedMetadata` instance."""
        return SerializedMetadata(self._version,
                                  self._function_namespaces.values(),
                                  self._data_item_namespaces.values(),
                                  self._parameters.values(),
                                  self._columns.values(),
                                  self._parameter_groups.values(),
                                  self._column_groups.values(),
                                  self._data_items.values(),
                                  self._functions.values(),
                                  self._universe_handlers.values(),
                                  self._operators,
                                  self._metadata_build_version)
