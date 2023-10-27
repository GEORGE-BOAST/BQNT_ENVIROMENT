# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.
import json
import logging

from .response_error import ResponseError, UnexpectedResponseError
from .types import convert_to_np_array, convert_to_pd_series

_logger = logging.getLogger(__name__)


class SingleItemResponse(object):
    """Represents BQL's response to a single data item in a :class:`Request`.

    Instances of :class:`Response` are made up of one or more 
    :class:`SingleItemResponse` objects.

    Example
    -------
    .. code-block:: python
        :linenos:
        :emphasize-lines: 10

        # Setup environment
        import bql
        bq = bql.Service()

        data_item = bq.data.name()

        # Instantiate and execute Request
        request = bql.Request('AAPL US Equity', data_item)
        response = bq.execute(request)
        single_item_response = response[0]

    """
    def __init__(self, result_dict, execution_context, name=None,
                 payload_id=None):
        self._execution_context = execution_context
        self._payload_id = payload_id
        self.__result_dict = result_dict
        self._name = name or result_dict['name']
        # TODO(aburgm): check for partial errors, raise exception if
        # corresponding preference is set in Context.

    def _sir(self):
        # Return the original SIR that we read from the JSON.
        return self.__result_dict

    @property
    def name(self):
        """The name of this single item response.

        By default, name values correspond to the string in the GET clause
        of the BQL query, but you can override this by constructing up the
        query string using named :class:`bql.Items`.
        """
        return self._name

    def df(self, tz_aware=False):
        """Unpack the data for this SingleItemResponse into a pandas DataFrame.

        Parameters
        ----------
        tz_aware : bool, optional
            If ``True``, :obj:`datetime` values in the DataFrame are 
            timezone-aware (UTC). The default is ``False``.

        Returns
        -------
        pandas.DataFrame
            Data returned by BQL

        Example
        -------
        .. code-block:: python
            :emphasize-lines: 11

            # Setup environment
            import bql
            bq = bql.Service()

            universe = bq.univ.members('INDU Index')
            data_item = bq.data.px_last(fill='prev')

            # Instantiate and execute Request
            request = bql.Request(universe, data_item)
            response = bq.execute(request)
            data = response[0].df()
            data.head()

        """
        return self.to_dataframe(tz_aware)

    def _make_column_name(self, column_data):
        if column_data is self.__result_dict['valuesColumn']:
            # Reset the name of the value column by the name of the SIR. This
            # is somewhat controversial, see BACC-1797.
            return self.name
        elif column_data['rank'] > 1:
            return f"{column_data['name']} ({column_data['rank']})"
        else:
            return column_data['name']

    def to_dataframe(self, tz_aware=False):
        return self.__from_json_blob(tz_aware)
    
    def __log_bql_exceptions(self, exception_list):
        if exception_list:
            error = ResponseError(exception_list, self._execution_context)
            # When logging, take advantage of the fact that the ResponseError
            # class formats the exception list for us.
            _logger.error("BQL ERROR: Error in evaluating '%s': %s, "
                          "request_id=%s",
                          self._name,
                          error,
                          error.request_id,
                          extra={'suppress': True})
            raise error

    def __from_json_blob(self, tz_aware=False):
        """
        Return a DataFrame with the content of this response.
        The datetime values in the dataframe are timezone aware (utc) if tz_aware
        is True, naive otherwise.
        """
        import pandas
        result = self.__result_dict

        response_exceptions = result.get('responseExceptions', None)
        self.__log_bql_exceptions(response_exceptions)

        # Note that the `idColumns` list will only consist of 0 or 1 items.
        # We treat it as a list to conveniently handle these two cases.
        if result['idColumn'] is not None:
            idColumns = [result["idColumn"]]
        else:
            idColumns = []

        secondaryColumns = result["secondaryColumns"] or []

        if result['valuesColumn'] is not None:
            valuesColumns = [result["valuesColumn"]]
        else:
            valuesColumns = []

        # Get name of columns
        idColumnNames = [self._make_column_name(col) for col in idColumns]
        nonIdColumns = secondaryColumns + valuesColumns
        nonIdColumnNames = [self._make_column_name(col)
                            for col in nonIdColumns]

        if tz_aware:
            ids = [convert_to_pd_series(column['values'], column['type'], col_name)
                   for column, col_name in zip(idColumns, idColumnNames)]
            data = [convert_to_pd_series(col['values'], col['type'], col_name)
                    for col_name, col in zip(nonIdColumnNames, nonIdColumns)]
        else:
            ids = [convert_to_np_array(column['values'], column['type']) for column
                   in idColumns]
            data = {col_name: convert_to_np_array(col['values'], col['type']) for
                    col_name, col in zip(nonIdColumnNames, nonIdColumns)}

        # Add partial errors, if any
        partial_error_map = result.get('partialErrorMap')
        if partial_error_map is not None:
            error_iterator = partial_error_map.get('errorIterator')
            if error_iterator is not None:
                # "errorIterator": [
                #   {
                #     "0": {
                #       "errorStack": [
                #         "error message here"
                #       ],
                #       "offset": 0
                #     }
                #   },
                #   {
                #     "1": ...
                #
                # Build up a {row: [error_msg]} dict to record all partial
                # error messages per row.
                error_msgs_by_row = {}
                for row_errors in error_iterator:
                    if len(row_errors) != 1:
                        raise UnexpectedResponseError(
                            "Expected only one row in partial error block: "
                            f"{row_errors}")

                    num, val = next(iter(row_errors.items()))
                    row = int(num)
                    error_stack = val["errorStack"]
                    error_msgs_by_row[row] = error_stack

                if idColumns:
                    n_rows = len(idColumns[0]['values'])
                elif nonIdColumns:
                    n_rows = len(nonIdColumns[0]['values'])
                else:
                    n_rows = max(error_msgs_by_row) + 1

                # Create a new column for the partial errors.
                if tz_aware:
                    data.append(convert_to_pd_series(
                        # Concatenate all the row error messages to put in the cell
                        ["\n".join(error_msgs_by_row.get(row, []))
                            for row in range(n_rows)],
                        'STRING', 'Partial Errors'))
                else:
                    data['Partial Errors'] = convert_to_np_array(
                        # Concatenate all the row error messages to put in the cell
                        ["\n".join(error_msgs_by_row.get(row, [])) for row in
                         range(n_rows)], 'STRING')
                nonIdColumnNames.append('Partial Errors')

        # TODO(kwhisnant): Index to more than just idColumn?

        # The nonIdColumnNames list is almost equivalent with data.keys(), but
        # it has a defined order, and so defines the order of columns in the
        # dataframe.
        assert len(ids) == len(idColumnNames)
        assert len(ids) <= 1
        if tz_aware:
            df = pandas.concat(ids + data, axis=1)
            if len(ids) != 0:
                df = df.set_index(keys=idColumnNames)
            return df
        else:
            if len(ids) == 0:
                return pandas.DataFrame(data,
                    columns=pandas.Index(nonIdColumnNames))
            return pandas.DataFrame(data, index=pandas.Index(ids[0],
                name=idColumnNames[0]), columns=pandas.Index(nonIdColumnNames))


    def _to_row_dicts(self):
        # Don't expose this publicly for now.
        # TODO(aburgm): should implement this without going the extra hoop
        # through pandas.
        # TODO: Looks like this function is not used. Maybe it can go.
        df = self.to_dataframe()
        return df.to_dict(orient="list")
