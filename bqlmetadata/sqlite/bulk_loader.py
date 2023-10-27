from ..metadata_serializer import (SerializedMetadata,
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

from .version_util import decode_version

import sqlite3

from collections import defaultdict


class BulkMetadataLoader(object):

    def __init__(self, db):
        self._db = db
        self._db.row_factory = sqlite3.Row

        self._enumerants = defaultdict(list)         # param_id -> [enum names]
        self._parameter_aliases = defaultdict(list)  # param_id -> [aliases]
        self._parameter_group_parameters = defaultdict(list)
        self._column_group_columns = defaultdict(list)
        self._func_names = defaultdict(list)
        self._func_parameter_groups = defaultdict(list)
        self._dataitem_names = defaultdict(list)
        self._dataitem_parameters = defaultdict(list)
        self._universe_handler_names = defaultdict(list)
        self._universe_handler_parameter_groups = defaultdict(list)

        self._enumerants = self._load_enumerants()
        self._parameter_aliases = self._load_parameter_aliases()
        self._parameter_group_parameters = (
            self._load_parameter_group_parameters())
        self._column_group_columns = self._load_column_group_columns()
        self._func_names = self._load_func_names()
        self._func_parameter_groups = self._load_func_parameter_groups()
        self._dataitem_names = self._load_dataitem_names()
        self._dataitem_parameters = self._load_dataitem_parameters()
        self._universe_handler_names = self._load_universe_handler_names()
        self._universe_handler_parameter_groups = (
            self._load_universe_handler_parameter_groups())

        self._version = self._load_version()
        self._operators = self._load_operators()
        self._columns = self._load_columns()
        self._parameters = self._load_parameters()
        self._parameter_groups = self._load_parameter_groups()
        self._column_groups = self._load_column_groups()
        self._functions = self._load_functions()
        self._dataitems = self._load_data_items()
        self._function_namespaces = self._load_namespaces("FunctionNamespaces")
        self._dataitem_namespaces = self._load_namespaces("DataItemNamespaces")
        self._universe_handlers = self._load_universe_handlers()

    def _load_enumerants(self):
        query = "SELECT name, parameter FROM Enumerants;"
        results = self._db.cursor().execute(query)

        for row in results:
            self._enumerants[row['parameter']].append(row['name'])

        return self._enumerants

    def _load_universe_handler_names(self):
        query = "SELECT universe_handler_id, name FROM UniverseHandlerNames;"
        results = self._db.cursor().execute(query)

        for row in results:
            id_ = row['universe_handler_id']
            self._universe_handler_names[id_].append(row['name'])

        return self._universe_handler_names

    def _load_func_names(self):
        query = "SELECT func_id, name FROM FunctionNames;"
        results = self._db.cursor().execute(query)

        for row in results:
            self._func_names[row['func_id']].append(row['name'])

        return self._func_names

    def _load_dataitem_names(self):
        query = "SELECT data_item_id, name FROM DataitemNames;"
        results = self._db.cursor().execute(query)

        for row in results:
            self._dataitem_names[row['data_item_id']].append(row['name'])

        return self._dataitem_names

    def _load_func_parameter_groups(self):
        query = "SELECT func, parameter_group FROM FunctionParameters;"
        results = self._db.cursor().execute(query)

        for row in results:
            self._func_parameter_groups[row['func']].append(
                row['parameter_group'])

        return self._func_parameter_groups

    def _load_dataitem_parameters(self):
        query = ("SELECT data_item, parameter_group, column_group "
                 "FROM DataitemParameters;")
        results = self._db.cursor().execute(query)

        for row in results:
            id_ = row['data_item']
            self._dataitem_parameters[id_].append((row['parameter_group'],
                                                  row['column_group']))

        return self._dataitem_parameters

    def _load_universe_handler_parameter_groups(self):
        query = ("SELECT universe_handler, parameter_group FROM "
                 "UniverseHandlerParameters;")
        results = self._db.cursor().execute(query)

        for row in results:
            id_ = row['universe_handler']
            self._universe_handler_parameter_groups[id_].append(
                row['parameter_group'])

        return self._universe_handler_parameter_groups

    def _load_data_items(self):
        query = ("SELECT id, name, namespace, mnemonic, "
                 "default_parameter_group, is_bulk, is_hidden, "
                 "is_periodic, description, availability FROM DataItems;")
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']

            aliases = (set(self._dataitem_names.get(id_, [])) -
                       set([row['name'], row['mnemonic']]))

            params = self._dataitem_parameters.get(id_, [])

            ret[id_] = SerializedMetadataDataItem(
                id_,
                row['name'],
                list(aliases),
                row['namespace'],
                row['mnemonic'],
                row['default_parameter_group'],
                row['is_bulk'],
                row['is_hidden'],
                row['is_periodic'],
                row['description'],
                params,
                row['availability'])

        return ret

    def _load_functions(self):
        query = ("SELECT id, name, namespace, is_hidden, is_aggregating, "
                 "mnemonic, description, return_type, macro_expression, "
                 "macro_processor, availability FROM Functions;")
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']
            # To get aliases, take all names and remove
            # the mnemonic and the name
            aliases = (set(self._func_names.get(id_, [])) -
                       set([row['name'], row['mnemonic']]))
            ret[id_] = SerializedMetadataFunction(
                id_,
                row['name'],
                list(aliases),
                row['namespace'],
                row['is_hidden'],
                row['is_aggregating'],
                row['mnemonic'],
                row['description'],
                row['return_type'],
                row['macro_expression'],
                row['macro_processor'],
                self._func_parameter_groups.get(id_, []),
                row['availability'])

        return ret

    def _load_parameter_aliases(self):
        query = "SELECT param_id, alias FROM ParameterAliases;"
        results = self._db.cursor().execute(query)

        for row in results:
            self._parameter_aliases[row['param_id']].append(row['alias'])

        return self._parameter_aliases

    def _load_parameter_group_parameters(self):
        query = ("SELECT parameter, parameter_group "
                 "FROM ParameterGroupParameters;")

        results = self._db.cursor().execute(query)

        for row in results:
            self._parameter_group_parameters[row['parameter_group']].append(
                row['parameter'])

        return self._parameter_group_parameters

    def _load_column_group_columns(self):
        query = "SELECT id, col, column_group FROM ColumnGroupColumns;"
        results = self._db.cursor().execute(query)

        for row in results:
            self._column_group_columns[row['column_group']].append(row['col'])

        return self._column_group_columns

    def _load_operators(self):
        query = "SELECT id, operator, func, location FROM Operators;"
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            ret[row['id']] = SerializedMetadataOperator(
                row['operator'], row['func'], row['location'])

        return ret

    def _load_version(self):
        query = "SELECT value FROM MetadataProperties WHERE name='version';"
        results = self._db.cursor().execute(query).fetchall()
        if not results or results[0]['value'] is None:
            return None

        return decode_version(int(results[0]['value']))

    def _load_columns(self):
        query = ("SELECT id, name, data_type, description, is_default "
                 "FROM Columns;")

        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            ret[row['id']] = SerializedMetadataColumn(
                row['id'],
                row['name'],
                row['data_type'],
                row['description'],
                row['is_default'])

        return ret

    def _load_parameters(self):
        query = ("SELECT p.id, p.name, p.data_type, p.description, "
                 "p.default_value, p.is_optional, p.is_list, "
                 "p.allows_multiple, p.enum_name FROM Parameters AS p;")

        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']

            enumerants = self._enumerants.get(id_, None)
            aliases = self._parameter_aliases.get(id_, [])

            ret[id_] = SerializedMetadataParameter(
                id_,
                row['name'],
                row['data_type'],
                row['description'],
                row['default_value'],
                row['is_optional'],
                row['is_list'],
                row['allows_multiple'],
                enumerants,
                row['enum_name'],
                aliases)

        return ret

    def _load_parameter_groups(self):
        query = "SELECT id, name, description FROM ParameterGroups;"
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']

            parameters = self._parameter_group_parameters.get(id_, [])

            ret[id_] = SerializedMetadataParameterGroup(
                id_,
                row['name'],
                row['description'],
                parameters)

        return ret

    def _load_column_groups(self):
        query = "SELECT id, name, description FROM ColumnGroups;"
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']
            columns = self._column_group_columns.get(id_, [])

            ret[id_] = SerializedMetadataColumnGroup(
                id_,
                row['name'],
                row['description'],
                columns)

        return ret

    def _load_namespaces(self, table):
        query = "SELECT id, name, full_path FROM {};".format(table)
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']
            ret[id_] = SerializedMetadataNamespace(
                id_,
                row['name'],
                row['full_path'],
                None)

        return ret

    def _load_universe_handlers(self):
        query = ("SELECT id, name, mnemonic, description, availability "
                 "FROM UniverseHandlers;")
        results = self._db.cursor().execute(query)

        ret = {}

        for row in results:
            id_ = row['id']
            ret[id_] = SerializedMetadataUniverseHandler(
                id_,
                row['name'],
                row['description'],
                self._universe_handler_parameter_groups.get(id_, []),
                row['availability']
            )

        return ret

    def load_metadata(self):
        return SerializedMetadata(
            self._version,
            self._function_namespaces.values(),
            self._dataitem_namespaces.values(),
            self._parameters.values(),
            self._columns.values(),
            self._parameter_groups.values(),
            self._column_groups.values(),
            self._dataitems.values(),
            self._functions.values(),
            self._universe_handlers.values(),
            self._operators.values())
