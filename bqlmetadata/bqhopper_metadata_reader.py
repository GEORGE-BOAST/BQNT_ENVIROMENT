# Copyright 2020 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from .exceptions import MetadataError
from .literals import make_parameter_literal_from_number
from .metadata import Parameter, ParameterGroup, MetadataItem
from .metadata_reader import MetadataReader
from .metadata_serializer import SerializedMetadataOperator
from .version_util import build_version_info_from_response


import logging

_logger = logging.getLogger(__name__)


def _build_parameters(metadata):
    parameters = {}

    for param_id, param in metadata['parameters'].items():
        for data_type, literal in [
            make_parameter_literal_from_number(param['data_type'],
                                               param['enum_name'],
                                               param['enumerants'])
        ]:
            try:
                parameters[param_id] = Parameter(param['id'],
                                                 param['name'],
                                                 data_type,
                                                 literal,
                                                 param['default_value'],
                                                 param['description'],
                                                 param['is_optional'],
                                                 param['param_type'],
                                                 param['aliases'])

            except MetadataError as e:
                _logger.exception(e, extra={'suppress': True})

    return parameters


def _build_parameter_groups(metadata, params):
    pgroups = {}

    for pgroup_name, pgroup in metadata['parameter-groups'].items():
        try:
            pg = ParameterGroup(
                pgroup['id'],
                pgroup['name'],
                *[params[param_id] for param_id in pgroup['parameters']])

            pgroups[pgroup_name] = pg

        except KeyError as e:
            _logger.info('Skipping pgroup %s due to invalid parameter.',
                         pgroup_name, exc_info=e)

        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return pgroups


def _get_output_cols(cgroup_ids, cgroup_map, cname_map):
    """Get a unique list of output column names given a set of
    column group ids."""
    col_names = set()

    for cg_name in cgroup_ids:
        col_names.update([cname_map[col] for col in cgroup_map[cg_name]])
    return list(col_names)


def _build_inv_operator_map(metadata):
    """Builds map to look up operators by their corresponding functions.
    """
    return {
        op_func_name: (op['operator'], op['location'])
        for op_func_name, op
        in metadata['operators'].items()
    }


def _build_colgroup_map(metadata):
    """Builds map to look up metadata columns names by column-group name.
    """
    return {
        col_group_name: col_group['columns']
        for col_group_name, col_group
        in metadata['column-groups'].items()
    }


def _build_colname_map(metadata):
    """Builds map to look up column names by canonical column name/id.
    """
    return {
        col_id: col['name']
        for col_id, col
        in metadata['columns'].items()
    }


def _build_data_items(metadata,
                      param_groups,
                      colgroup_map,
                      colname_map,
                      metadata_reader):
    data_items = {}

    for item_name, item in metadata['data-items'].items():

        pgroups = []
        colgroup_names = set()

        for pgroup_name, colgroup_name in item['parameter_groups']:
            colgroup_names.add(colgroup_name)

            try:
                pgroups.append(param_groups[pgroup_name])

            except KeyError as e:
                _logger.info('Skipping invalid pgroup %s',
                             pgroup_name, exc_info=e)

        try:
            output_cols = _get_output_cols(colgroup_names,
                                           colgroup_map,
                                           colname_map)

            data_item = MetadataItem.create_data_item(item['id'],
                                                      item['name'],
                                                      item['mnemonic'],
                                                      item['description'],
                                                      pgroups,
                                                      output_cols,
                                                      metadata_reader,
                                                      item['availability'])
            data_items[item_name] = data_item

        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return data_items


def _build_functions(metadata,
                     param_groups,
                     inv_operator_map,
                     metadata_reader):
    functions = {}

    for func_name, func in metadata['functions'].items():

        pgroups = []

        for pgroup_name in func['parameter_groups']:

            try:
                pgroups.append(param_groups[pgroup_name])

            except KeyError as e:
                _logger.info('Skipping invalid pgroup %s',
                             pgroup_name, exc_info=e)

        try:
            op_name, op_loc = inv_operator_map.get(func_name, (None, None))
            function = MetadataItem.create_function(
                func['id'],
                func['name'],
                func['mnemonic'],
                func['description'],
                pgroups,
                op_name,
                op_loc,
                metadata_reader,
                func['availability'])

            functions[func_name] = function

        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return functions


def _build_universe_handlers(metadata, param_groups, metadata_reader):
    univ_handlers = {}

    for uvhd_name, uvhd in metadata['universe-handlers'].items():

        pgroups = []

        for pgroup_name in uvhd['parameter_groups']:

            try:
                pgroups.append(param_groups[pgroup_name])

            except KeyError as e:
                _logger.info('Skipping invalid pgroup %s',
                             pgroup_name, exc_info=e)

        try:
            # TODO (dgoltra2): replace name indexing to uvhd['name'] once
            #  new cut of bqhopper deployed
            uh = MetadataItem.create_universe_handler(
                uvhd['id'],
                uvhd.get('keyword') or uvhd.get('name'),
                None,
                uvhd['description'],
                pgroups,
                metadata_reader,
                uvhd['availability'])

            univ_handlers[uvhd_name] = uh

        except MetadataError as e:
            _logger.exception(e, extra={'suppress': True})

    return univ_handlers


def _build_operator_map(metadata, functions):
    operator_map = {}

    for func_name, op in metadata['operators'].items():

        try:
            operator_map[
                (op['operator'], op['location'])] = functions[func_name]

        except KeyError as e:
            _logger.info('Skipping operator %s due to invalid function.',
                         op['operator'], exc_info=e)

    return operator_map


def _map_items_by_name(items, serialized_items):
    result = {}

    for item_name, item in serialized_items.items():

        try:
            obj = items[item_name]
            result.setdefault(item['name'].lower(), []).append(obj)

            if (item['mnemonic'] is not None
                    and item['mnemonic'].lower() != item['name'].lower()):

                result.setdefault(
                    item['mnemonic'].lower(), []).append(obj)

            for alias in item['aliases']:
                result.setdefault(alias.lower(), []).append(obj)

        except KeyError as e:
            _logger.info('Skipping item %s', item_name, exc_info=e)

    return result


def _map_universe_handlers_by_name(universe_handlers,
                                   serialized_universe_handlers):
    universe_handlers_by_name = {}

    for uvhd_name, uvhd in serialized_universe_handlers.items():

        try:
            uh = universe_handlers[uvhd_name]
            universe_handlers_by_name[uvhd_name] = [uh]

        except KeyError as e:
            _logger.info('Skipping universe handler %s',
                         uvhd_name, exc_info=e)

    return universe_handlers_by_name


class BqhopperMetadataReader(MetadataReader):
    def __init__(self, deserializer):
        metadata_payload = deserializer.load_metadata()
        metadata = metadata_payload['metadata']

        self._parameters = _build_parameters(metadata)

        self._pgroups = _build_parameter_groups(metadata, self._parameters)
        # Look up operators by their functions
        inv_operator_map = _build_inv_operator_map(metadata)

        self._colgroup_map = _build_colgroup_map(metadata)

        self._colname_map = _build_colname_map(metadata)

        self._data_items = _build_data_items(metadata,
                                             self._pgroups,
                                             self._colgroup_map,
                                             self._colname_map,
                                             self)

        self._functions = _build_functions(metadata,
                                           self._pgroups,
                                           inv_operator_map,
                                           self)

        self._universe_handlers = _build_universe_handlers(metadata,
                                                           self._pgroups,
                                                           self)

        # Lookup Tables
        self._operator_map = _build_operator_map(metadata, self._functions)

        self._functions_by_name = _map_items_by_name(
            self._functions,
            metadata['functions'])

        self._data_items_by_name = _map_items_by_name(
            self._data_items,
            metadata['data-items'])

        # TODO (dgoltra2): Refactor once ENG2BQNTFL-2824 completed
        self._universe_handlers_by_name = _map_universe_handlers_by_name(
            self._universe_handlers,
            metadata['universe-handlers'])

        self._version_info = build_version_info_from_response(
            metadata.get('version-info'))

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
            'operators': self._operator_map,
        }

    def get_version(self):
        return self._version_info
