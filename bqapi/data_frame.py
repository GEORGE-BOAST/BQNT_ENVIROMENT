# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from itertools import chain

from .dictionary import dictionary
from .utils import scalar_or_list
from .format import Format


# We use the output of dict format and construct a dataframe with it. This is a very
# generic approach; if it turns out to be too slow, we can optimize common usecases
# separately.


class _DataFrameOutputFormat(object):

    def __init__(self, dict_format, default_index_func, override_index=None,
                 override_columns=None, override_drop_duplicates=None):
        self._dict_format = dict_format
        self._default_index_func = default_index_func

        self._override_index = override_index
        self._override_columns = override_columns
        self._override_drop_duplicates = override_drop_duplicates

    def update(self, message):
        self._dict_format.update(message)

    def finalize(self):
        import pandas

        dict_output = self._dict_format.finalize()

        index, columns, drop_duplicates, field_index, transpose = self.get_index(
            dict_output)

        dataframe = pandas.DataFrame(dict_output)
        dataframe.columns.set_names('Field', inplace=True)
        if not dict_output:
            return pandas.DataFrame(dict_output)

        # First, drop all rows that would cause a duplicate index.
        if drop_duplicates and len(index) + len(columns) > 0:
            dataframe.drop_duplicates(chain(index, columns), inplace=True)

        # Next, set an all-row index
        indices = list(chain(index, reversed(columns)))
        if indices:
            dataframe.set_index(indices, inplace=True)

        # Unstack the index for each column index, thereby moving the index from the
        # rows (as set by set_index above) to the columns
        for _ in columns:
            dataframe = dataframe.unstack()

        # Re-order the original column index to the right level
        for x in range(field_index):
            dataframe = dataframe.swaplevel(x, x + 1, axis=1)

        # Sort the index, drop columns with only NaN values which could result from unstacking
        # Use mergesort instead of quicksort, because it seems to be implemented in such a way
        # that it is stable. If, for example, we have intraday tick data with multiple ticks per
        # second, this keeps the relative order of the ticks within that second.
        dataframe.sort_index(axis=0, inplace=True, kind='mergesort')
        dataframe.sort_index(axis=1, inplace=True, kind='mergesort')

        # The axis=0 case hits a weird bug in pandas:
        # https://github.com/pydata/pandas/issues/13407
        if dataframe.shape[1] == 1:
            dataframe = pandas.DataFrame(
                dataframe.iloc[:, 0].dropna(), columns=dataframe.columns)
        else:
            dataframe.dropna(axis=0, how='all', inplace=True)
        dataframe.dropna(axis=1, how='all', inplace=True)

        # Transpose the result if columns and rows were supposed to be swapped
        if transpose:
            dataframe = dataframe.T
        return dataframe

    def get_index(self, dict_output):
        if self._override_index is None or self._override_columns is None or self._override_drop_duplicates is None:
            index, columns, drop_duplicates = self._default_index_func(
                dict_output)
            if self._override_index is not None:
                index = self._override_index
            if self._override_columns is not None:
                columns = self._override_columns
            if self._override_drop_duplicates is not None:
                drop_duplicates = self._override_drop_duplicates
        else:
            index, columns, drop_duplicates = self._override_index, self._override_columns, self._override_drop_duplicates

        index = list(scalar_or_list(index))
        columns = list(scalar_or_list(columns))
        transpose = False

        # Determine where in the index the data columns go (the ones that are
        # are not going to be used as an index). By default, we put them as the
        # last level of the column index, but the placeholder '_' can be used
        # to re-arrange them.
        assert index.count('_') + columns.count('_') < 2

        if '_' in index:
            field_index = index.index('_')
            index.remove('_')

            transpose = True
            index, columns = columns, index
        elif '_' in columns:
            field_index = columns.index('_')
            columns.remove('_')
        else:
            # Default: Fields are last level of column index
            field_index = len(columns)

        return index, columns, drop_duplicates, field_index, transpose


class _DataFrameFormatFactory(Format):

    def __init__(self, default_index_func_func, index, columns, drop_duplicates):
        # TODO: can we make this a bit less meta?
        self._default_index_func_func = default_index_func_func
        self._index = index
        self._columns = columns
        self._drop_duplicates = drop_duplicates

    def make_format(self, request_type, *args, **kwargs):
        """Create the format object."""
        format = dictionary.make_format(request_type, *args, **kwargs)
        default_index_func = self._default_index_func_func(
            request_type, args, kwargs)
        return _DataFrameOutputFormat(format, default_index_func, self._index, self._columns, self._drop_duplicates)


class _DataFrameFormatFactoryDefault(Format):

    def __init__(self):
        self._default_index = {}

    def get_default_index(self, request_type, hook_args, hook_kwargs):
        if request_type not in self._default_index:

            def default_default_index(output_dict):
                return [], [], False

            return default_default_index
        else:
            default_index_func = self._default_index[request_type]

            def custom_default_index(output_dict):
                return default_index_func(output_dict, *hook_args, **hook_kwargs)

            return custom_default_index

    def register_default_index(self, request_type):
        """Specify the default index for a given request type.

        The decorated function gets the output from the :attr:`dictionary` formatter
        as first parameter, and the positional and keyword arguments that were passed
        to :meth:`Session.make_request` as remaining parameters. It should return a
        3-tuple of ``(row_index, columns, drop_duplicates)``. In this tuple,
        `row_index` is a string or a list of strings specifying the column to use
        as an index. The special value ``'_'`` can be used to refer to the queried
        fields. If it is a list, a multi index is created. The `columns' component
        specifies the index to use for the other (column) axis. If the special value
        ``'_'`` is not given in either `row_index` or `columns`, it is implicitly
        appended to the column index. The `drop_duplicates` component is a boolean
        value which specifies whether rows with all values the same in the index columns
        should be dropped. In that case, only the last occuring row is kept.

        The values returned by this function are used as default values when no
        other index is specified by calling the :attr:`data_frame` formatter.
        """
        def decorator(func):
            if request_type in self._default_index:
                raise AttributeError(
                    'There is already a default index for requests of type "{}"'.format(request_type))
            self._default_index[request_type] = func
            return func
        return decorator

    def __call__(self, index=None, columns=None, drop_duplicates=None):
        """Specify index and column index for the data frame."""
        return _DataFrameFormatFactory(self.get_default_index, index, columns, drop_duplicates)

    def make_format(self, request_type, *args, **kwargs):
        """Create the format object."""
        format = dictionary.make_format(request_type, *args, **kwargs)
        default_index_func = self.get_default_index(request_type, args, kwargs)
        return _DataFrameOutputFormat(format, default_index_func)

    def __repr__(self):
        # Mostly to show something useful in the documentation
        return '{}()'.format(self.__class__.__name__)


data_frame = _DataFrameFormatFactoryDefault()
"""Return result as a :class:`pandas.DataFrame`.

This formatter produces a :class:`pandas.DataFrame` for the result. Each field
queried will be available as a column, and the implementation will choose a
meaningful index for the data frame.

The data frame formatter can be called to choose a particular index and alter
the construction of the returned dataframe otherwise. The following
parameters are defined.

Parameters
----------
    index : str, list
        The column or columns to use as the index. If this parameter is a list of strings,
        a multi-index is created. The special value ``'_'`` can be used to refer
        to the queried fields.
    columns : str, list
        The column or columns to use as the index for the other (column) axis.
        If this parameter is a list of strings, a multi-index is created.
        The special value ``'_'`` can be used to refer to the queried fields.
    drop_duplicates : bool
        If this parameter is set to `True`, all rows except one with the same
        values in all index columns are dropped. The last occuring row is kept.
"""
