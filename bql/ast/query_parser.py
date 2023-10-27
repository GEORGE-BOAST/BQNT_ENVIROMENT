# Copyright 2019 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from abc import ABCMeta, abstractmethod

from six import with_metaclass

from .bql_graph import BqlGraph
from ..request.client_context import ClientContextBuilder


class QueryParserError(Exception):
    pass


class QueryParser(with_metaclass(ABCMeta)):
    svc_name = 'bqldsvc'
    request_name = 'getEditableGraphRequest'

    @staticmethod
    def _create_request(bql_expression):
        return dict(
            expression=bql_expression,
            clientContext=ClientContextBuilder().get_context()
        )

    def parse_to_ast(self, bql_query):
        """
        Given a bql string query, return a BqlGraph instance containing the
        Abstract Syntax Tree of the query.
        """
        request = self._create_request(bql_query)
        response = self._send_request(request)
        return self._get_graph_from_response(response)

    def parse_to_om(self, bql_string_query, metadata_reader):
        """
        Given a bql string query, return an instance of the bql.Request
        class corresponding to the query.
        """
        ast = self.parse_to_ast(bql_string_query)
        return ast.to_request(metadata_reader)

    @staticmethod
    def _check_response(response):
        if len(response) != 1:
            raise QueryParserError("Incorrect response from"
                                   " BQL Discovery Service")
        response = response[0]
        # The errorResponse from bqapi ends up being a str instead of a dict
        if isinstance(response, str):
            raise QueryParserError(response)
        elif 'errorResponse' in response:
            raise QueryParserError(response['errorResponse'])
        elif 'nodes' not in response:
            raise QueryParserError("'nodes' not found in response from "
                                   "BQL Discovery Service")
        return response

    def _get_graph_from_response(self, response):
        response = self._check_response(response)
        return BqlGraph.ast_from_nodes(response['nodes'])

    @abstractmethod
    def _send_request(self, request):
        pass
