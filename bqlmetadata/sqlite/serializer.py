#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import itertools
import os

from .version_util import encode_version
from ..metadata_serializer import MetadataSerializer


_this_path = os.path.abspath(os.path.dirname(__file__))


class SqliteMetadataSerializer(MetadataSerializer):
    """Seralizes metadata into an SQLite database."""

    def __init__(self, db):
        self._db = db

        # Create database if not yet created
        cursor = self._db.cursor()
        version, = cursor.execute('PRAGMA user_version').fetchone()
        if version < 1:
            schema_path = os.path.join(_this_path, 'schema.sql')
            with open(schema_path) as f:
                schema = f.read()
            cursor.executescript(schema)
            self._db.commit()

    def store_metadata(self, metadata):
        """Stores metadata in the sqlite db

        Parameters
        -----------
        metadata: an instance of SerializedMetadata
        """

        with self._db:
            # Import version
            if metadata.version is not None:
                encoded_version = encode_version(*metadata.version)
            else:
                encoded_version = None

            self._db.executemany(
                'INSERT INTO MetadataProperties (name, value) '
                'VALUES (?, ?)', [
                    ("version", encoded_version),
                    ("metadata_build_version", metadata.metadata_build_version)
                ])

            # Import namespaces
            function_namespaces = [
                (namespace.id,
                 namespace.name,
                 namespace.full_path,
                 namespace.parent_namespace)
                for namespace in metadata.function_namespaces
            ]

            self._db.executemany(
                'INSERT INTO FunctionNamespaces (id, name, full_path, parent) '
                'VALUES (?, ?, ?, ?)', function_namespaces)

            data_item_namespaces = [
                (namespace.id,
                 namespace.name,
                 namespace.full_path,
                 namespace.parent_namespace)
                for namespace in metadata.data_item_namespaces
            ]

            self._db.executemany(
                'INSERT INTO DataItemNamespaces (id, name, full_path, parent) '
                'VALUES (?, ?, ?, ?)', data_item_namespaces)

            # Import parameters.  See SerializedMetadataParameter for how
            # param_type can be loaded into the is_list column.
            parameters = [
                (parameter.id,
                 parameter.name,
                 parameter.data_type,
                 parameter.description,
                 parameter.default_value,
                 parameter.is_optional,
                 parameter.param_type,
                 parameter.allows_multiple,
                 parameter.enum_name) for parameter in metadata.parameters]

            self._db.executemany(
                'INSERT INTO Parameters (id, name, data_type, description, '
                'default_value, is_optional, is_list, allows_multiple, '
                'enum_name) VALUES (?,?,?,?,?,?,?,?,?)', parameters)

            # Import parameter aliases.
            param_aliases = []
            for param in metadata.parameters:
                for alias in param.aliases:
                    param_aliases.append((param.id, alias))

            self._db.executemany(
                'INSERT INTO ParameterAliases (param_id, alias) VALUES (?,?)',
                param_aliases)

            # Import enumerants
            enumerants = [
                (enumerant, parameter.id)
                for parameter in metadata.parameters
                for enumerant in
                (parameter.enumerants if parameter.enumerants else [])
            ]

            self._db.executemany(
                'INSERT INTO Enumerants (name, parameter) VALUES (?,?)',
                enumerants)

            # Import columns
            columns = [
                (column.id,
                 column.name,
                 column.data_type,
                 column.description,
                 column.is_default) for column in metadata.columns
            ]

            self._db.executemany(
                'INSERT INTO Columns (id, name, data_type, description, '
                'is_default) VALUES (?,?,?,?,?)', columns)

            # Import parameter groups
            parameter_groups = [
                (pgroup.id, pgroup.name, pgroup.description)
                for pgroup in metadata.parameter_groups
            ]

            self._db.executemany(
                'INSERT INTO ParameterGroups (id, name, description) '
                'VALUES (?,?,?)', parameter_groups)

            parameter_group_parameters = [
                (param, pgroup.id)
                for pgroup in metadata.parameter_groups
                for param in pgroup.parameters
            ]

            self._db.executemany(
                'INSERT INTO ParameterGroupParameters (parameter, '
                'parameter_group) VALUES (?,?)', parameter_group_parameters)

            # Import column groups
            column_groups = [
                (cgroup.id, cgroup.name, cgroup.description)
                for cgroup in metadata.column_groups
            ]

            self._db.executemany(
                'INSERT INTO ColumnGroups (id, name, description) '
                'VALUES (?,?,?)', column_groups)

            column_group_columns = [(column, cgroup.id)
                                    for cgroup in metadata.column_groups
                                    for column in cgroup.columns]

            self._db.executemany(
                'INSERT INTO ColumnGroupColumns (col, column_group) '
                'VALUES (?,?)', column_group_columns)

            # Import functions
            functions = [
                (func.id,
                 func.name,
                 func.namespace,
                 func.is_hidden,
                 func.is_aggregating,
                 func.mnemonic,
                 func.description,
                 func.return_type,
                 func.macro_expression,
                 func.macro_processor,
                 func.availability) for func in metadata.functions
            ]

            self._db.executemany(
                'INSERT INTO Functions (id, name, namespace, is_hidden, '
                'is_aggregating, mnemonic, description, return_type, '
                'macro_expression, macro_processor, availability) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?)', functions)

            function_parameters = [(func.id, pgroup)
                                   for func in metadata.functions
                                   for pgroup in func.parameter_groups]

            self._db.executemany(
                'INSERT INTO FunctionParameters (func, parameter_group) '
                'VALUES (?,?)', function_parameters)

            function_names = [
                (func.id, name)
                for func in metadata.functions
                for name in itertools.chain(
                        [func.name, func.mnemonic], func.aliases)
                if name is not None and name != ''
            ]

            self._db.executemany(
                'INSERT INTO FunctionNames (func_id, name) VALUES (?, ?)',
                list(set(function_names)))

            # Import data items
            data_items = [
                (item.id,
                 item.name,
                 item.namespace,
                 item.mnemonic,
                 item.default_parameter_group,
                 item.is_bulk,
                 item.is_hidden,
                 item.is_periodic,
                 item.description,
                 item.availability) for item in metadata.data_items
            ]

            self._db.executemany(
                'INSERT INTO DataItems (id, name, namespace, mnemonic, '
                'default_parameter_group, is_bulk, is_hidden, is_periodic, '
                'description, availability) VALUES (?,?,?,?,?,?,?,?,?,?)',
                data_items)

            data_item_parameters = [
                (item.id, pgroup, cgroup)
                for item in metadata.data_items
                for pgroup, cgroup in item.parameter_groups
            ]

            self._db.executemany(
                'INSERT INTO DataItemParameters (data_item, parameter_group, '
                'column_group) VALUES (?,?,?)', data_item_parameters)

            data_item_names = [
                (item.id, name)
                for item in metadata.data_items
                for name in itertools.chain(
                        [item.name, item.mnemonic],
                        item.aliases)
                if name is not None and name != '']

            self._db.executemany(
                'INSERT INTO DataItemNames (data_item_id, name) VALUES (?, ?)',
                (set(data_item_names)))

            # Import universe handlers
            universe_handlers = [
                (uvhd.id, uvhd.keyword, uvhd.description, uvhd.availability)
                for uvhd in metadata.universe_handlers
            ]

            self._db.executemany(
                'INSERT INTO UniverseHandlers '
                    '(id, name, mnemonic, description, availability) '
                    'VALUES (?,?,NULL,?,?)',
                universe_handlers)

            universe_handler_parameters = [
                (uvhd.id, pgroup)
                for uvhd in metadata.universe_handlers
                for pgroup in uvhd.parameter_groups
            ]

            self._db.executemany(
                'INSERT INTO UniverseHandlerParameters (universe_handler, '
                'parameter_group) VALUES (?,?)', universe_handler_parameters)

            universe_handler_names = [
                (uvhd.id, uvhd.keyword) for uvhd in metadata.universe_handlers
            ]

            self._db.executemany(
                'INSERT INTO UniverseHandlerNames (universe_handler_id, name) '
                'VALUES (?,?)', universe_handler_names)

            # Import operators
            operators = [
                (op.operator, op.function, op.location)
                for op in metadata.operators
            ]

            self._db.executemany(
                'INSERT INTO Operators (operator, func, location) '
                'VALUES (?,?,?)', operators)
