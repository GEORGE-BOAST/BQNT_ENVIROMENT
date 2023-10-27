# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import bz2
import json
import six

from .metadata_serializer import (MetadataDeserializer,
                                  MetadataSerializer,
                                  SerializedMetadata,
                                  SerializedMetadataColumn,
                                  SerializedMetadataColumnGroup,
                                  SerializedMetadataDataItem,
                                  SerializedMetadataFunction,
                                  SerializedMetadataNamespace,
                                  SerializedMetadataOperator,
                                  SerializedMetadataParameter,
                                  SerializedMetadataParameterGroup,
                                  SerializedMetadataUniverseHandler,
                                  VersionInfo)


class _BqapiElementJSONEncoder(json.JSONEncoder):
    # When the metadata is read through bqapi, the response elements are
    # wrapped in bqapi classes; json.dump() needs to know how to process
    # these wrapped elements.
    def default(self, item):
        from bqapi.element import DictElement, ListElement
        if isinstance(item, DictElement):
            results = {}
            for (key, value) in item.items():
                if isinstance(value, DictElement):
                    results[key] = dict(value)
                elif isinstance(value, ListElement):
                    results[key] = list(value)
                else:
                    results[key] = value
            return results
        elif isinstance(item, ListElement):
            return list(item)
        else:
            return super().default(item)


class MinJsonMetadataSerializer(MetadataSerializer):
    """Serializes metadata into a minified json representation."""

    def __init__(self, filename):
        self._filename = filename

    def store_metadata(self, metadata):
        function_namespaces = [
            [ns.id, ns.name, ns.full_path, ns.parent_namespace]
            for ns in metadata.function_namespaces
        ]
        data_item_namespaces = [
            [ns.id, ns.name, ns.full_path, ns.parent_namespace]
            for ns in metadata.data_item_namespaces
        ]
        parameters = [[
            param.id,
            param.name,
            param.data_type,
            param.description,
            param.default_value,
            param.is_optional,
            param.param_type,
            param.allows_multiple,
            param.enumerants,
            param.enum_name,
            param.aliases
        ] for param in metadata.parameters]
        columns = [[
            column.id,
            column.name,
            column.data_type,
            column.description,
            column.is_default
        ] for column in metadata.columns]
        parameter_groups = [[
            pgroup.id,
            pgroup.name,
            pgroup.description,
            pgroup.parameters
        ] for pgroup in metadata.parameter_groups]
        column_groups = [[
            cgroup.id,
            cgroup.name,
            cgroup.description,
            cgroup.columns
        ] for cgroup in metadata.column_groups]
        functions = [[
            func.id,
            func.name,
            func.aliases,
            func.namespace,
            func.is_hidden,
            func.is_aggregating,
            func.mnemonic,
            func.description,
            func.return_type,
            func.macro_expression,
            func.macro_processor,
            func.parameter_groups,
            func.availability
        ] for func in metadata.functions]
        data_items = [[
            item.id,
            item.name,
            item.aliases,
            item.namespace,
            item.mnemonic,
            item.default_parameter_group,
            item.is_bulk,
            item.is_hidden,
            item.is_periodic,
            item.description,
            item.parameter_groups,
            item.availability
        ] for item in metadata.data_items]
        universe_handlers = [[
            uvhd.id,
            uvhd.keyword,
            uvhd.description,
            uvhd.parameter_groups,
            uvhd.availability
        ] for uvhd in metadata.universe_handlers]
        operators = [
            [op.operator, op.function, op.location]
            for op in metadata.operators
        ]
        result = {
            'version': metadata.version,
            'function-namespaces': function_namespaces,
            'data-item-namespaces': data_item_namespaces,
            'parameters': parameters,
            'columns': columns,
            'parameter-groups': parameter_groups,
            'column-groups': column_groups,
            'functions': functions,
            'data-items': data_items,
            'universe-handlers': universe_handlers,
            'operators': operators
        }
        if six.PY2:
            with bz2.BZ2File(self._filename, 'w') as f:
                json.dump(result, f, cls=_BqapiElementJSONEncoder)
        else:
            with bz2.open(self._filename, 'wt') as f:
                json.dump(result, f, cls=_BqapiElementJSONEncoder)


class MinJsonMetadataDeserializer(MetadataDeserializer):
    def __init__(self, filename):
        if six.PY2:
            with bz2.BZ2File(filename, 'r') as f:
                self.__loaded = json.load(f)
        else:
            with bz2.open(filename, 'r') as f:
                self.__loaded = json.load(f)
        if self.__loaded['version'] is not None:
            self.__version = tuple(self.__loaded['version'])
        else:
            self.__version = None

    def _load_metadata(self, prev_version):
        function_namespaces = [SerializedMetadataNamespace(*ns)
                               for ns in self.__loaded['function-namespaces']]
        data_item_namespaces = [SerializedMetadataNamespace(*ns)
                                for ns in self.__loaded['data-item-namespaces']]
        parameters = [SerializedMetadataParameter(*param)
                      for param in self.__loaded['parameters']]
        columns = [SerializedMetadataColumn(*column)
                   for column in self.__loaded['columns']]
        parameter_groups = [SerializedMetadataParameterGroup(*pgroup)
                            for pgroup in self.__loaded['parameter-groups']]
        column_groups = [SerializedMetadataColumnGroup(*cgroup)
                         for cgroup in self.__loaded['column-groups']]
        functions = [SerializedMetadataFunction(*func)
                     for func in self.__loaded['functions']]
        data_items = []
        for item in self.__loaded['data-items']:
            # The 'parameter_groups' argument to SerializedMetadataItem
            # (the 10th positional argument) should be a list of tuples:
            # (parameter group, column group) association pairs.  But when
            # we parse from JSON, they are decoded as lists.
            item[10] = [tuple(assoc) for assoc in item[10]]
            data_items.append(SerializedMetadataDataItem(*item))
        universe_handlers = [SerializedMetadataUniverseHandler(*uvhd)
                             for uvhd in self.__loaded['universe-handlers']]
        operators = [SerializedMetadataOperator(*op)
                     for op in self.__loaded['operators']]

        return SerializedMetadata(self.__version,
                                  function_namespaces, data_item_namespaces,
                                  parameters, columns,
                                  parameter_groups, column_groups,
                                  data_items, functions, universe_handlers,
                                  operators)

    def _get_version(self):
        # Serialized metadata is no longer associated with a BQL metadata
        # schema version.
        schema_version = None
        return VersionInfo(self.__version, schema_version, None)
