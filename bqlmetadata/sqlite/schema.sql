-- Copyright 2016 Bloomberg Finance L.P.
-- All Rights Reserved.
-- This software is proprietary to Bloomberg Finance L.P. and is
-- provided solely under the terms of the BFLP license agreement.

CREATE TABLE FunctionNamespaces(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	full_path TEXT NOT NULL COLLATE NOCASE,
	parent INTEGER REFERENCES FunctionNamespaces ON DELETE CASCADE);

CREATE TABLE DataItemNamespaces(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	full_path TEXT NOT NULL COLLATE NOCASE,
	parent INTEGER REFERENCES DataItemNamespaces ON DELETE CASCADE);

CREATE TABLE Columns(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	data_type INTEGER NOT NULL,
	description TEXT COLLATE NOCASE,
	is_default INTEGER NOT NULL);

CREATE TABLE ColumnGroups(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	description TEXT COLLATE NOCASE);

CREATE TABLE ColumnGroupColumns(
	id INTEGER PRIMARY KEY,
	col INTEGER NOT NULL REFERENCES Columns,
	column_group INTEGER NOT NULL REFERENCES ColumnGroups ON DELETE CASCADE);

CREATE TABLE Parameters(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	data_type INTEGER NOT NULL,
	description TEXT COLLATE NOCASE,
	default_value TEXT,
	is_optional INTEGER NOT NULL,
    -- is_list is one of the SerializedMetadataParameter.LIST_TYPE constants;
    -- it's not just a Boolean flag.
	is_list INTEGER NOT NULL,
	allows_multiple INTEGER NOT NULL,
	enum_name TEXT);

CREATE TABLE Enumerants(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	parameter INTEGER NOT NULL References Parameters ON DELETE CASCADE);

CREATE TABLE ParameterGroups(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	description TEXT COLLATE NOCASE);

CREATE TABLE ParameterGroupParameters(
	id INTEGER PRIMARY KEY,
	parameter INTEGER NOT NULL REFERENCES Parameters ON DELETE CASCADE,
	parameter_group INTEGER NOT NULL REFERENCES ParameterGroups ON DELETE CASCADE);

CREATE TABLE DataItems(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	namespace INTEGER REFERENCES DataItemNamespaces ON DELETE CASCADE,
	mnemonic TEXT COLLATE NOCASE,
	default_parameter_group REFERENCES ParameterGroups ON DELETE CASCADE,
	is_bulk INTEGER NOT NULL,
	is_hidden INTEGER NOT NULL,
	is_periodic INTEGER NOT NULL,
	description TEXT COLLATE NOCASE,
	availability INTEGER NOT NULL);

CREATE TABLE DataItemNames(
	data_item_id INTEGER REFERENCES DataItems ON DELETE CASCADE,
	name TEXT NOT NULL COLLATE NOCASE
);

CREATE TABLE DataItemParameters(
	id INTEGER PRIMARY KEY,
	data_item INTEGER NOT NULL REFERENCES DataItems ON DELETE CASCADE,
	parameter_group INTEGER NOT NULL REFERENCES ParameterGroups ON DELETE CASCADE,
	column_group INTEGER NOT NULL REFERENCES ColumnGroups ON DELETE CASCADE);

CREATE TABLE Functions(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	namespace INTEGER REFERENCES FunctionNamespaces ON DELETE CASCADE,
	is_hidden INTEGER NOT NULL,
	is_aggregating INTEGER NOT NULL,
	mnemonic TEXT COLLATE NOCASE,
	description TEXT COLLATE NOCASE,
	return_type INTEGER NOT NULL,
	macro_expression TEXT COLLATE NOCASE,
	macro_processor TEXT COLLATE NOCASE,
	availability INTEGER NOT NULL);

CREATE TABLE FunctionNames(
	func_id INTEGER REFERENCES Functions ON DELETE CASCADE,
	name TEXT NOT NULL COLLATE NOCASE
);

CREATE TABLE FunctionParameters(
	id INTEGER PRIMARY KEY,
	func INTEGER NOT NULL REFERENCES Functions ON DELETE CASCADE,
	parameter_group INTEGER NOT NULL REFERENCES ParameterGroups ON DELETE CASCADE);

-- For universe handlers, note that the name represents the universe
-- handler "keyword", and mnemonic is always NULL. This structure allows us
-- to use the same code for universe handlers as for data items and functions.
CREATE TABLE UniverseHandlers(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL COLLATE NOCASE,
	mnemonic TEXT COLLATE NOCASE,
	description TEXT COLLATE NOCASE,
	availability INTEGER NOT NULL);

CREATE TABLE UniverseHandlerNames(
	universe_handler_id INTEGER REFERENCES UniverseHandlers ON DELETE CASCADE,
	name TEXT NOT NULL COLLATE NOCASE);

CREATE TABLE UniverseHandlerParameters(
	id INTEGER PRIMARY KEY,
	universe_handler INTEGER NOT NULL REFERENCES UniverseHandlers ON DELETE CASCADE,
	parameter_group INTEGER NOT NULL REFERENCES ParameterGroups ON DELETE CASCADE);

CREATE TABLE Operators(
	id INTEGER PRIMARY KEY,
	operator TEXT COLLATE NOCASE,
	func INTEGER NOT NULL REFERENCES Functions ON DELETE CASCADE,
	location INTEGER NOT NULL);

CREATE TABLE MetadataProperties(
	name TEXT PRIMARY KEY NOT NULL,
	value TEXT);

CREATE TABLE ParameterAliases(
    -- Ideally we'd make param_id a foreign key, but the parameters are split
    -- across DataItemParameters, FunctionParameters, and UniverseHandlers for
    -- historical reasons (separate parameter ID namespaces originally).  It's
    -- more convenient to have all parameter aliases in one table, so we're
    -- doing without the foreign key.
    param_id INTEGER NOT NULL,
    alias STRING NOT NULL);

-- TODO: Is this still needed if we enable FTS5 for these tables and columns?
CREATE INDEX FunctionNameIndex ON FunctionNames(name);
CREATE INDEX DataItemNameIndex ON DataItemNames(name);
CREATE INDEX UniverseHandlerNameIndex ON UniverseHandlerNames(name);

-- Create indices on most foreign key child columns. This is suggested by the
-- SQLite documentation and probably helps with JOINing tables by their
-- foreign keys, and when updating the metadata.
-- Leave out namespaces for now, as there are typically only few, and when
-- listing all functions in a particular namespace we need to scan large parts
-- of the table already anyway.
CREATE INDEX ColumnGroupColumnColumnIndex ON ColumnGroupColumns(col);
CREATE INDEX ColumnGroupColumnColumnGroupIndex ON ColumnGroupColumns(column_group);
CREATE INDEX ParameterGroupParameterParameterIndex ON ParameterGroupParameters(parameter);
CREATE INDEX ParameterGroupParameterParameterGroupIndex ON ParameterGroupParameters(parameter_group);
CREATE INDEX DataItemDefaultParameterGroupIndex ON DataItems(default_parameter_group);
CREATE INDEX DataItemParameterDataItemIndex ON DataItemParameters(data_item);
CREATE INDEX DataItemParameterParameterGroupIndex ON DataItemParameters(parameter_group);
CREATE INDEX DataItemParameterColumnGroupIndex ON DataItemParameters(column_group);
CREATE INDEX FunctionParameterFunctionIndex ON FunctionParameters(func);
CREATE INDEX FunctionParameterParameterGroupIndex ON FunctionParameters(parameter_group);
CREATE INDEX UniverseHandlerParameterUniverseHandlerIndex ON UniverseHandlerParameters(universe_handler);
CREATE INDEX UniverseHandlerParameterParameterGroupIndex ON UniverseHandlerParameters(parameter_group);
CREATE UNIQUE INDEX ParameterAliasesIndex ON ParameterAliases(param_id, alias);

-- Version history:
--    1: Initial schema
--    2: Factor out name from DataItems and Functions into alias tables.
--    3: Separate namespaces DataItems and Functions
--    4: Remove precision and scale from DataItems table
--    5: Add operators table
--    6: Add universe handlers
--    7: Add MetadataProperties table
--    8: Add enum_name to Parameters table
--    9: Add ParameterAliases table
--   10: Add availability column to DataItems, Functions, and UniverseHandlers
PRAGMA user_version = 10;
