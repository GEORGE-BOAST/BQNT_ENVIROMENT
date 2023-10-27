# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .exceptions import MetadataError
from .literals import make_parameter_literal_from_number
from .metadata import Parameter, ParameterGroup, MetadataItem
from .metadata_reader import MetadataReader
from .metadata_serializer import SerializedMetadataOperator
from .version_util import VersionInfo

import logging

_logger = logging.getLogger(__name__)


def _build_parameters(metadata):
    parameters = {}
    for param in metadata.parameters:
        for data_type, literal in [
                make_parameter_literal_from_number(param.data_type,
                                                   param.enum_name,
                                                   param.enumerants)
        ]:
            try:
                parameters[param.id] = Parameter(param.id, param.name,
                                                 data_type, literal,
                                                 param.default_value,
                                                 param.description,
                                                 param.is_optional,
                                                 param.param_type,
                                                 param.aliases)
            except MetadataError as e:
                _logger.exception(e, extra={'suppress': True})

    return parameters


def _build_parameter_groups(metadata, params):
    pgroups = {}

    for pgroup in metadata.parameter_groups:
        try:
            pg = ParameterGroup(pgroup.id,
                                pgroup.name,
                                *[params[id_] for id_ in pgroup.parameters])
            pgroups[pgroup.id] = pg
        except KeyError as e:
            _logger.info("Skipping pgroup %s due to invalid parameter.",
                         pgroup.id, exc_info=True)
        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return pgroups


def _build_data_items(metadata,
                      param_groups,
                      colgroup_map,
                      colname_map,
                      metadata_reader):
    data_items = {}

    for item in metadata.data_items:

        pgroups = []
        colgroup_ids = set()

        for pgroup_id, colgroup_id in item.parameter_groups:
            colgroup_ids.add(colgroup_id)
            try:
                pgroups.append(param_groups[pgroup_id])
            except KeyError as e:
                _logger.info("Skipping invalid pgroup %s",
                             pgroup_id, exc_info=True)

        try:
            output_cols = MemoryMetadataReader._get_output_cols(colgroup_ids,
                                                                colgroup_map,
                                                                colname_map)

            data_item = MetadataItem.create_data_item(item.id,
                                                      item.name,
                                                      item.mnemonic,
                                                      item.description,
                                                      pgroups,
                                                      output_cols,
                                                      metadata_reader,
                                                      item.availability)
            data_items[item.id] = data_item

        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return data_items


def _build_functions(metadata,
                     param_groups,
                     inv_operator_map,
                     metadata_reader):

    functions = {}

    for func in metadata.functions:

        pgroups = []

        for pgroup_id in func.parameter_groups:
            try:
                pgroups.append(param_groups[pgroup_id])
            except KeyError as e:
                _logger.info("Skipping invalid pgroup %s",
                             pgroup_id, exc_info=True)

        try:
            function = MetadataItem.create_function(
                func.id,
                func.name,
                func.mnemonic,
                func.description,
                pgroups,
                inv_operator_map.get(func.id, (None, None))[0],
                inv_operator_map.get(func.id, (None, None))[1],
                metadata_reader,
                func.availability)
            functions[func.id] = function
        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return functions


def _build_universe_handlers(metadata, param_groups, metadata_reader):

    univ_handlers = {}

    for uvhd in metadata.universe_handlers:

        pgroups = []

        for pgroup_id in uvhd.parameter_groups:
            try:
                pgroups.append(param_groups[pgroup_id])
            except KeyError as e:
                _logger.info("Skipping invalid pgroup %s",
                             pgroup_id, exc_info=True)

        try:
            uh = MetadataItem.create_universe_handler(
                uvhd.id,
                uvhd.keyword,
                None,
                uvhd.description,
                pgroups,
                metadata_reader,
                uvhd.availability)
            univ_handlers[uvhd.id] = uh
        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return univ_handlers


def _map_items_by_name(items, serialized_items):
    result = {}

    for item in serialized_items:
        try:
            obj = items[item.id]
            result.setdefault(item.name.lower(), []).append(obj)

            if (item.mnemonic is not None
                    and item.mnemonic.lower() != item.name.lower()):

                result.setdefault(
                    item.mnemonic.lower(), []).append(obj)

            for alias in item.aliases:
                result.setdefault(alias.lower(), []).append(obj)
        except KeyError as e:
            _logger.info("Skipping item %s", item.id, exc_info=True)

    return result


class MemoryMetadataReader(MetadataReader):
    """A metadata reader that reads metadata from memory."""
    def __init__(self, metadata):
        """
        Create a `MemoryMetadataReader` based on `SerializedMetadata`.
        `metadata` is a `SerializedMetadata` instance.
        """
        self._parameters = _build_parameters(metadata)
        self._pgroups = _build_parameter_groups(metadata, self._parameters)
        # Be able to look up operators by their functions.
        inv_operator_map = {
            op.function: (op.operator, op.location)
            for op in metadata.operators
        }
        # create column groups/names maps now since we don't want to do a
        # linear scan for each data item.
        #
        # Note: when metadata is loaded from JSON files these maps are
        # already present, but the loader is flattened into a
        # SerializedMetaData instance (which just strips the values of these
        # maps).
        colgroup_map = {cg.id: cg.columns for cg in metadata.column_groups}
        colname_map = {col.id: col.name for col in metadata.columns}
        self._data_items = _build_data_items(
            metadata, self._pgroups, colgroup_map, colname_map, self)
        self._functions = _build_functions(
            metadata, self._pgroups, inv_operator_map, self)
        self._universe_handlers = _build_universe_handlers(
            metadata, self._pgroups, self)
        # Lookup tables.
        self._operator_map = {}
        for op in metadata.operators:
            try:
                self._operator_map[
                    (op.operator, op.location)] = self._functions[op.function]
            except KeyError as e:
                _logger.info("Skipping operator %s due to invalid function.",
                             op.operator, exc_info=True)
        self._functions_by_name = _map_items_by_name(self._functions,
                                                     metadata.functions)
        self._data_items_by_name = _map_items_by_name(self._data_items,
                                                      metadata.data_items)
        # Universe handlers don't have name or mnemonic, but keywords.
        # The also don't have aliases. Assume there are no overlapping
        # universe handler keywords.
        self._universe_handlers_by_name = {}
        for obj in metadata.universe_handlers:
            try:
                uh = self._universe_handlers[obj.id]
                self._universe_handlers_by_name[obj.keyword.lower()] = [uh]
            except KeyError as e:
                _logger.info("Skipping universe handler %s",
                             obj.id, exc_info=True)
        self._version_info = VersionInfo(
            version=metadata.version,
            schema_version=None,
            metadata_build_version=metadata.metadata_build_version
        )

    @staticmethod
    def _get_output_cols(cgroup_ids, cgroup_map, cname_map):
        """Get a unique list of output column names given a set of
        column group ids."""
        colnames = set()
        for cid in cgroup_ids:
            colnames.update([cname_map[col] for col in cgroup_map[cid]])
        return list(colnames)

    def _get_by_name(self, what, name):
        result = []

        if 'data-item' in what:
            result.extend(self._data_items_by_name.get(name.lower(), []))

        if 'function' in what:
            result.extend(self._functions_by_name.get(name.lower(), []))

        if 'universe-handler' in what:
            result.extend(self._universe_handlers_by_name.get(
                name.lower(), []))

        return result

    def _get_by_operator(self, symbol, location):

        location_map = {
            'prefix': SerializedMetadataOperator.PREFIX,
            'infix': SerializedMetadataOperator.INFIX
        }

        return self._operator_map.get((symbol, location_map[location]))

    def _enumerate_names(self, what):
        result = set()

        if 'data-item' in what:
            result.update(self._data_items_by_name.keys())

        if 'function' in what:
            result.update(self._functions_by_name.keys())

        if 'universe-handler' in what:
            result.update(self._universe_handlers_by_name.keys())

        return result

    def get_bulk_metadata(self):
        return {
            'parameters': self._parameters,
            'parameter_groups': self._pgroups,
            'functions': self._functions,
            'data_items': self._data_items,
            'universe_handlers': self._universe_handlers,
            'operators': self._operator_map
        }

    def get_version(self):
        return self._version_info
