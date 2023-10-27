# Copyright 2019 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import datetime
import logging

import dateutil.parser

import bql

from collections import Counter

logger = logging.getLogger(__name__)


def _convert_date(date):
    if isinstance(date, datetime.date):
        return date
    date = date[:-1] if date.endswith('Z') else date
    return dateutil.parser.parse(date)


def _convert_special_val(val):
    if val.lower() in ['na', 'nan']:
        return float('nan')
    return val


_scalar_dispatch = {
    'doubleVal': float,
    'intVal': int,
    'booleanVal': bool,
    'relativeDateVal': lambda d: str(d.get('frq', '')) + d.get('frqType', ''),
    'absoluteDateVal': _convert_date,
    'enumVal': lambda d: d['enumValue'],
    'specialVal': _convert_special_val,
    None: str
}


class BqlGraph(dict):
    def __init__(self, *args, **kwargs):
        self.root = None
        super(BqlGraph, self).__init__(*args, **kwargs)

    @classmethod
    def ast_from_nodes(cls, nodes):
        """Create BqlGraph instance representing the AST for the given nodes"""
        tree = cls()
        # Add all the nodes to the tree with the node ids as the keys.
        for node in nodes:
            tree.add_node(node)

        # The children of the root are the BQL clauses
        _root, clauses = tree._get_keyword(tree[tree.root])
        for clause in clauses:
            _name, keyword_node = tree.extract_parameter(clause)
            if _name:
                logger.warning("Unexpected name found in keywordNode: {}",
                               _name)
            clause_name, parameters = tree._get_keyword(keyword_node)

            # Add each BQL clause to the tree using the clause name as the key
            tree[clause_name] = parameters
        return tree

    def add_node(self, node):
        """Add a node to the graph"""
        node_id, node_impl = node['id'], node['nodeImpl']
        if ('keywordNode' in node_impl
                and node_impl['keywordNode']['name'] == 'ROOT'):
            self.root = node_id
        self[node_id] = node_impl

    @staticmethod
    def _get_keyword(node):
        keyword = node['keywordNode']
        return keyword['name'], keyword['children']

    def extract_parameter(self, param):
        """Extract the parameter name and value from the given parameter node"""
        return param.get('paramName'), self[param['paramValueRef']['id']]
    
    def walk_keyword(self, keyword, process_func=None):
        """
        Walk the subtree for the given BQL keyword.

        Optionally supply a processing function (process_func) which will be
        called with each node as the tree is traversed and whose return value
        will be yielded instead of value of the node itself.
        """
        for param in self.get(keyword, ()):
            parameter = self.extract_parameter(param)
            yield process_func(*parameter) if process_func else parameter

    def to_request(self, metadata_reader):
        """
        Convert this AST to a BQL Request object using the given service object.
        """
        return _TreeToRequestConverter(self, metadata_reader).convert()


class _TreeToRequestConverter:
    def __init__(self, ast, metadata_reader):
        self.ast = ast
        self.metadata_reader = metadata_reader

    def convert(self):
        """Perform the conversion of ast to a BQL Request object"""
        query = []
        mappings = {'WITH', 'PREFERENCES'}
        for keyword in ('FOR', 'GET', 'WITH', 'PREFERENCES'):
            clause = {} if keyword in mappings else []
            parameters = self.ast.walk_keyword(keyword, self.process_param)
            for param_name, param in parameters:
                if keyword in mappings:
                    clause[param_name] = param
                else:
                    clause.append(param)
            query.append(clause)
        return bql.Request(*query)

    def item_node(self, item):
        item_type, data = next(iter(item.items()))
        name = data['name']
        parameters = data['parameters']
        if item_type == 'functionNode':
            bqlitem = self.create_bqlitem('function', name, parameters)
        elif item_type == 'dataitemNode':
            bqlitem = self.create_bqlitem('data-item', name, parameters)
        elif item_type == 'macroNode':
            bqlitem = self.create_bqlitem(('function', 'data-item'),
                                          name, parameters)
        else:
            raise ValueError(f'Unknown ItemNode type: {item_type}')
        col = data.get('selectedColumn')
        if col:
            bqlitem = bqlitem[col]
        return bqlitem

    # noinspection PyMethodMayBeStatic
    def scalar_node(self, item):
        item_type, data = next(iter(item.items()))
        return _scalar_dispatch.get(item_type, _scalar_dispatch[None])(data)

    def universe_node(self, item):
        name = item['name']
        parameters = item['parameters']
        return self.create_bqlitem('universe-handler', name,
                                   parameters, use_name=False)
    
    def range_node(self, item):
        name = 'range'
        parameters = item['parameters']
        return self.create_bqlitem('function', name, parameters)
            
    def list_node(self, item):
        # Throw away the parameter names and return a list of just parameter
        # values
        return [self.process_param(*self.ast.extract_parameter(p))[1]
                for p in item['parameters']]

    node_dispatch = {
        'itemNode': item_node,
        'scalarNode': scalar_node,
        'universeNode': universe_node,
        'rangeNode': range_node,
        'listNode': list_node
    }

    def process_param(self, name, param):
        node_type, item = next(iter(param['valueNode'].items()))
        return name, self.node_dispatch[node_type](self, item)
    
    def create_bqlitem(self, item_type, name, parameters, use_name=True):
        metadata = self.metadata_reader.get_by_name(item_type, name)
        item_factory = bql.om.BqlItemFactory(name, metadata)
        kwargs_count = Counter()
        kwargs_items = []
        args = []
        for param in parameters:
            name, value = self.process_param(*self.ast.extract_parameter(param))
            if use_name and name:
                kwargs_count[name] += 1
                kwargs_items.append((name, value))
            else:
                args.append(value)

        kwargs = {}
        if use_name:
            # Add to args if duplicate keys are found.
            for name, value in kwargs_items:
                if kwargs_count[name] == 1:
                    kwargs[name] = value
                else:
                    args.append(value)
        return item_factory(*args, **kwargs)
