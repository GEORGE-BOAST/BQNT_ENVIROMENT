# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import six
import datetime
import pandas as pd

from collections import OrderedDict

from ._default_service import _get_service

from ..request import Request, combined_df
from ..om import (InvalidParameterGroupError, InvalidParameterValueError,
                  InvalidParameterNameError)


def get_time_series(tickers, fields, start, end=None, service=None):
    """Return the time series of the fields for the specified tickers.

    Parameters
    ----------
    tickers: str, sequence
        A single ticker, or a sequence of tickers.
    fields: str, sequence
        A single field name, or a list of field names.
    start: datetime.date, str
        start date as :class:`datetime.date` or as a string in
        YYYY-MM-DD format.
    end: datetime.date, str (default: ``None``)
        end date as :class:`datetime.date` or as a string in
        YYYY-MM-DD format.
    service: :class:`bql.Service` (default: ``None``)
        the service to use to make the request, or ``None`` to use a default
        service.

    Returns
    -------
    :class:`pandas.DataFrame`
        A dataframe with a multi level index of ticker and date and
        field names as columns.
    """
    bq = _get_service(service)
    
    if isinstance(tickers, six.string_types):
        tickers = [tickers]
    
    if isinstance(fields, six.string_types):
        fields = [fields]

    if end is None:
        end = datetime.date.today()

    def make_item(field_name):
        try:
            item_class = getattr(bq.data, field_name)
        except AttributeError:
            raise ValueError(f'No such field with name "{field_name}"')

        try:
            item_obj = item_class(start=start, end=end)
        except InvalidParameterGroupError as ex:
            name_error, value_error, other_error = None, None, None
            for error in ex.individual_errors:
                if isinstance(error, InvalidParameterValueError):
                    value_error = error
                elif isinstance(error, InvalidParameterNameError):
                    name_error = error
                else:
                    other_error = error

            if value_error is not None:
                # Probably start or end are not properly formatted
                raise ValueError(f'Date format not recognized: {value_error}')
            elif name_error is not None:
                # Probably the item does not support start or end parameters
                raise ValueError(f'Field with name "{field_name}" does not '
                                 f'support time series: {name_error}')
            elif other_error is not None:
                raise ValueError(f'Field with name "{field_name}" cannot '
                                 f'be constructed: {other_error}')

        return item_obj

    req = Request(tickers, OrderedDict([(f, make_item(f)) for f in fields]))
    res = bq.execute(req)

    df = combined_df(res)
    df = df.dropna(subset=fields, how='all', axis=0)

    # drop non-value columns if they are all NaN. Note that we cannot do that
    # using df.dropna(axis=1) because that would also include the value columns.
    extra_columns = set(df.columns) - set(fields) - set(['DATE'])
    for column in extra_columns:
        coldata = df[column]
        if pd.isnull(coldata).all():
            df = df.drop(column, axis=1)

    df = df.reset_index().set_index(['ID', 'DATE'])
    return df
