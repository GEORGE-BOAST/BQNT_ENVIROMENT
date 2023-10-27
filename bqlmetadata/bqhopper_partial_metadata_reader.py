# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from collections import defaultdict

from .bqhopper_metadata_reader import BqhopperMetadataReader
from .exceptions import MetadataError, StaleMetadataError
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


def _get_output_cols(cgroup_names, cgroup_map, cname_map):
    """Get a unique list of output column names given a set of
    column group names.
    """
    col_names = set()

    for cg_name in cgroup_names:
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
    """Builds map to look up column names by canonical column id.
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


def _map_items_by_name(items, serialized_items, entity_names=None):
    result = defaultdict(list)

    for name in entity_names or []:
        result[name] = []

    for item_name, item in serialized_items.items():

        try:
            obj = items[item_name]
            result[item['name'].lower()].append(obj)

            if (item['mnemonic'] is not None
                    and item['mnemonic'].lower() != item['name'].lower()):

                result[item['mnemonic'].lower()].append(obj)

            for alias in item['aliases']:
                result[alias.lower()].append(obj)

        except KeyError as e:
            _logger.info('Skipping item %s', item_name, exc_info=e)

    return dict(result)


def _map_universe_handlers_by_name(universe_handlers,
                                   serialized_universe_handlers,
                                   univ_handler_names=None):
    result = defaultdict(list)

    for name in univ_handler_names or []:
        result[name] = []

    for uvhd_name, uvhd in serialized_universe_handlers.items():

        try:
            uh = universe_handlers[uvhd_name]
            result[uvhd_name].append(uh)

        except KeyError as e:
            _logger.info('Skipping universe handler %s',
                         uvhd_name, exc_info=e)

    return dict(result)


def _get_canonical_entity_names(entities, entity_type):
    return [
        entity['name'].lower()
        for entity in entities
        if entity['type'] == entity_type
    ]


class BqhopperPartialMetadataReader(MetadataReader):
    def __init__(self, deserializer):
        # Storing the deserializer as a member variable as we will need
        # it to make a getMetadataForEntityRequest to the bqhopper service.
        self._deserializer = deserializer
        self._create_partial_metadata_reader()

    def _create_partial_metadata_reader(self):
        metadata_payload = self._deserializer.load_partial_metadata()
        metadata = metadata_payload['metadata']
        entities = metadata_payload.get('entities', [])

        # The server-side cache key where the partial metadata is stored.
        # Required to make the getMetadataForEntityRequest.
        self._partial_metadata_id = metadata_payload.get('partial_metadata_id')

        # Contains all canonical names of data-items
        # i.e. names, mnemonics, and aliases
        data_item_names = _get_canonical_entity_names(entities, 'DATA_ITEM')
        function_names = _get_canonical_entity_names(entities, 'FUNCTION')
        univ_handler_names = _get_canonical_entity_names(entities,
                                                         'UNIVERSE_FUNCTION')

        self._parameters = _build_parameters(metadata)

        self._pgroups = _build_parameter_groups(metadata, self._parameters)

        self._inv_operator_map = _build_inv_operator_map(metadata)

        self._colgroup_map = _build_colgroup_map(metadata)

        self._colname_map = _build_colname_map(metadata)

        self._data_items = _build_data_items(metadata,
                                             self._pgroups,
                                             self._colgroup_map,
                                             self._colname_map,
                                             self)

        self._functions = _build_functions(metadata,
                                           self._pgroups,
                                           self._inv_operator_map,
                                           self)

        self._universe_handlers = _build_universe_handlers(metadata,
                                                           self._pgroups,
                                                           self)

        # Lookup Tables
        # TODO (dgoltra2): Move this as we have enough info to
        #  build this map from entities in the getPartialMetdataResponse; I.E.
        #  we don't need metadata payload to build this map.
        # TODO (dgoltra2): Do we need inv_operator_map and operator_map?
        self._operator_map = {
            (
                e['operator']['symbol'], e['operator']['location']
            ): e['operator']['function']
            for e
            in entities
            if e['type'] == 'FUNCTION' and e.get('operator')
        }

        # This will contain all the canonical data-item names as keys with
        # either the corresponding metadata for the data-items we have in
        # self._data_items as values or an empty list for names with missing
        # metadata. As one-off requests are made for names with missing
        # metadata, both self._data_items and self._data_items_by_name will be
        # updated.
        self._data_items_by_name = _map_items_by_name(
            self._data_items,
            metadata['data-items'],
            data_item_names)

        self._functions_by_name = _map_items_by_name(
            self._functions,
            metadata['functions'],
            function_names)

        # TODO (dgoltra2): Refactor once ENG2BQNTFL-2824 completed
        self._universe_handlers_by_name = _map_universe_handlers_by_name(
            self._universe_handlers,
            metadata['universe-handlers'],
            univ_handler_names)

        self._version_info = build_version_info_from_response(
            metadata.get('version-info'))

    def _update_metadata_with_entity(self, entity_md, entity_type):

        # Update parameters
        self._parameters.update(_build_parameters(entity_md))

        # Update parameter-groups
        self._pgroups.update(
            _build_parameter_groups(entity_md, self._parameters))

        # Update operator map
        self._inv_operator_map.update(_build_inv_operator_map(entity_md))

        # Update column-groups map
        self._colgroup_map.update(_build_colgroup_map(entity_md))

        # Update columns map
        self._colname_map.update(_build_colname_map(entity_md))

        # There should only be one entity
        if entity_type == 'DATA_ITEM':
            entity = self._update_data_items(entity_md)

        elif entity_type == 'FUNCTION':
            entity = self._update_functions(entity_md)

        elif entity_type == 'UNIVERSE_FUNCTION':
            entity = self._update_univ_handlers(entity_md)

        else:
            raise ValueError(f'Invalid entity type: "{entity_type}"')

        return entity

    def _update_data_items(self, serialized_entity_md):
        """Updates the _data_items and _data_items_by_name dicts of the reader
        with a single, new data-item with its associative metadata fetched
        from bqhopper and returns its pybql object model representation.

        Parameters
        ----------
        serialized_entity_md: dict
            The full metadata payload for a single, new data-item entity
            and its associations.

        Returns
        -------
        data_item: class MetadataItem
            The pybql object model representation of the new bql data-item
            fetched from bqhopper.
        """
        new_data_items = _build_data_items(serialized_entity_md,
                                           self._pgroups,
                                           self._colgroup_map,
                                           self._colname_map,
                                           self)
        # Should be only a single data-item.
        data_item, *_ = new_data_items.values()
        self._data_items.update(new_data_items)

        # Update _data_items_by_name dict with new name, mnemonics, and aliases.
        new_data_items_by_name = _map_items_by_name(
            self._data_items,
            serialized_entity_md['data-items'])
        self._data_items_by_name.update(new_data_items_by_name)

        return data_item

    def _update_functions(self, serialized_entity_md):
        """Updates the _functions and _functions_by_name dicts of the reader
        with a single, new function with its associative metadata fetched
        from bqhopper and returns its pybql object model representation.

        Parameters
        ----------
        serialized_entity_md: dict
            The full metadata payload for a single, new function entity and its
            associations.

        Returns
        -------
        function: class MetadataItem
            The pybql object model representation of the new bql function
            fetched from bqhopper.
        """
        new_functions = _build_functions(serialized_entity_md,
                                         self._pgroups,
                                         self._inv_operator_map,
                                         self)
        # Should be only a single function.
        function, *_ = new_functions.values()
        self._functions.update(new_functions)

        # Update _functions_by_name dict with new name, mnemonics, and aliases.
        new_functions_by_name = _map_items_by_name(
            self._functions,
            serialized_entity_md['functions'])
        self._functions_by_name.update(new_functions_by_name)

        return function

    def _update_univ_handlers(self, serialized_entity_md):
        """Updates the _universe_handlers and _universe_handlers_by_name dicts
        of the reader with a single, new universe-handlers with its associative
        metadata fetched from bqhopper and returns its pybql object model
        representation.

        Parameters
        ----------
        serialized_entity_md: dict
            The full metadata payload for a single, new universe-handler entity
            and its associations.

        Returns
        -------
        univ_handler: class MetadataItem
            The pybql object model representation of the new bql
            universe-handler fetched from bqhopper.
        """
        new_univ_handlers = _build_universe_handlers(serialized_entity_md,
                                                     self._pgroups,
                                                     self)
        # Should be only a single universe handler.
        univ_handler, *_ = new_univ_handlers.values()
        self._universe_handlers.update(new_univ_handlers)

        # Update _universe_handlers_by_name dict with new names (keywords).
        new_univ_handlers_by_name = _map_universe_handlers_by_name(
            self._universe_handlers,
            serialized_entity_md['universe-handlers'])
        self._universe_handlers_by_name.update(new_univ_handlers_by_name)

        return univ_handler

    def _get_by_name(self, what, name):
        name = name.lower()
        result = []

        if 'data-item' in what and name in self._data_items_by_name:
            data_items = self._data_items_by_name[name]

            if data_items:
                data_item = data_items[0]

            else:
                try:
                    entity_md = self._deserializer.get_metadata_for_entity(
                        name,
                        self._partial_metadata_id,
                        'DATA_ITEM')

                except StaleMetadataError:
                    # Exception raised when the version of the metadata we
                    # cache is different than the version of the metadata
                    # entity we just fetched. If this happens, We need to
                    # refresh the reader with fresh partial metadata
                    self._create_partial_metadata_reader()
                    entity_md = self._deserializer.get_metadata_for_entity(
                        name,
                        self._partial_metadata_id,
                        'DATA_ITEM')
                data_item = self._update_metadata_with_entity(entity_md,
                                                              'DATA_ITEM')

            result.append(data_item)

        if 'function' in what and name in self._functions_by_name:
            functions = self._functions_by_name[name]

            if functions:
                function = functions[0]

            else:
                try:
                    entity_md = self._deserializer.get_metadata_for_entity(
                        name,
                        self._partial_metadata_id,
                        'FUNCTION')

                except StaleMetadataError:
                    self._create_partial_metadata_reader()
                    entity_md = self._deserializer.get_metadata_for_entity(
                        name,
                        self._partial_metadata_id,
                        'FUNCTION')
                function = self._update_metadata_with_entity(entity_md,
                                                             'FUNCTION')

            result.append(function)

        if ('universe-handler' in what
                and name in self._universe_handlers_by_name):
            univ_handlers = self._universe_handlers_by_name[name]

            if univ_handlers:
                univ_handler = univ_handlers[0]

            else:
                try:
                    entity_md = self._deserializer.get_metadata_for_entity(
                        name,
                        self._partial_metadata_id,
                        'UNIVERSE_FUNCTION')

                except StaleMetadataError:
                    self._create_partial_metadata_reader()
                    entity_md = self._deserializer.get_metadata_for_entity(
                        name,
                        self._partial_metadata_id,
                        'UNIVERSE_FUNCTION')
                univ_handler = self._update_metadata_with_entity(
                    entity_md,
                    'UNIVERSE_FUNCTION')

            result.append(univ_handler)

        return result

    def _get_by_operator(self, symbol, location):
        location_map = {
            'prefix': SerializedMetadataOperator.PREFIX,
            'infix': SerializedMetadataOperator.INFIX
        }

        op_func = self._operator_map.get((symbol, location_map[location]))

        return self._get_by_name('function', op_func)[0]

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
        # TODO (dgoltra2): Revisit updating partial reader state when method called
        reader = BqhopperMetadataReader(self._deserializer)
        return reader.get_bulk_metadata()

    def get_version(self):
        return self._version_info
