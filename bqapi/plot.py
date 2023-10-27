# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import itertools
import numbers
import datetime

import six

from .utils import scalar_or_list
from .dictionary import dictionary
from .format import Format


# TODO: Think about some more generic data binding framework the plot framework could build up on:
#   This particular approach has the limitation that there can only be one target
# class DataBindingFormat(object):
#    def __init__(self, dict_format, target, attr, converter = None):
#        self.dict_format = dict_format
#        self.target = target
#        self.attr = attr
#        self.converter = converter
#
#        # No converter: Identity function
#        if self.converter is None:
#            self.converter = lambda x: x

#    def update(self, message):
#        self.dict_format.update(message)

#    def finalize(self):
#        output = self.dict_format.finalize()
#        setattr(self.target, self.attr, self.converter(output))


class DictPlotAdaptor(object):

    """Create columns of x and y values from a list of dictionaries.

    This class is an adaptor to turn the output of the :attr:`dictionary`
    formatter to what the :attr:`plot` formatter is producing and passing
    to the plotter backend. It can be used if data is needed both in the
    form of numbers and a visualization. In that case, one would use
    :attr:`dictionary` as a formatter, and pass the dictionary to the
    :meth:`DictPlotAdaptor.plot` method to create the figure.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~DictPlotAdaptor.__init__
        ~DictPlotAdaptor.plot

    .. automethod:: DictPlotAdaptor.__init__
    """

    def __init__(self, plotter, x_columns, y_columns, group_by=None):
        """Create a new adaptor.

        Creates a new adaptor. Takes the same arguments as :attr:`plot`, except that
        no default values are accepted for `x_columns`, `y_columns` or `group_by`.

        Parameters
        ----------
            plotter : callable
                A callable which is called with a list of dictionaries. Each dictionary has
                5 values: ``'x-label'``, ``'y-label'`` are strings which describe the X and Y
                values. They can be used by the backend as axis labels, or ignored if axis
                labels are obtained elsewhere. The ``'group'`` item is a tuple with one string
                for each entry in the `group_by` parameter, and it specifies which group this
                data series corresponds to. The ``'x'`` and ``'y'`` items are numpy arrays with
                x and y values. The callable should return an instance representing the
                visualized plot, such as bitmap or vector graphics data.
            x_columns : str, list
                Specifies the column(s) to use as x values.
            y_columns : str, list
                Specifies the column(s) to be used as y values.
            group_by : str, list
                Lists columns which should be treated as different groups and different plots be
                created. For example, if this is ``'Security'``, a separate plot is created for
                each security in the result.
        """
        self._plotter = plotter
        self._group_by = group_by

        # Use the same X column for all Y columns if not specified differently
        # Also, convert to lists so we can iterate them as often as we want
        self.y_columns = list(scalar_or_list(y_columns))
        if isinstance(y_columns, (list, tuple)) and not isinstance(x_columns, (list, tuple)):
            self.x_columns = [x_columns] * len(self.y_columns)
        else:
            self.x_columns = list(scalar_or_list(x_columns))

        if not self.x_columns:
            raise ValueError('No column for X data specified')
        if not self.y_columns:
            raise ValueError('No column for Y data specified')

    def plot(self, dict_output):
        """Create a figure based on the output of the :attr:`dictionary` formatter.

        This function turns the output of the :attr:`dictionary` output formatter to
        a list of x and y values and calls the adaptor's plotter with it. Depending
        on what backend that plotter is using, a figure visualizing the data is being
        generated.

        Parameters
        ----------
            dict_output : dict
                A list of dictionaries as produced by the :attr:`dictionary` formatter.
        """
        # The format we give to the plotter is the following:
        # [{ 'x-label': name of x column, 'y-label': name of y column, 'group': name of group, 'x': x points, 'y': y points }]

        plots = []
        if self._group_by is not None:
            # Sort by groups
            def group(row):
                return tuple([row[group_name] for group_name in scalar_or_list(self._group_by)])

            dict_output = sorted(dict_output, key=group)

            for grp, rows in itertools.groupby(dict_output, key=group):
                # So that we can iterate over them multiple times
                rows = list(rows)
                plots.extend(self._process_rows(rows, grp))
        else:
            plots.extend(self._process_rows(dict_output, tuple()))

        return self._plotter(plots)

    def _process_rows(self, rows, group):
        def to_numpy_array(val):
            import numpy

            if len(val) == 0:
                return numpy.array()
            # Note it is important that we check first datetime and then date,
            # since datetime is also a date.
            elif isinstance(val[0], datetime.datetime):
                return numpy.array(val, dtype='datetime64[us]')
            elif isinstance(val[0], datetime.date):
                return numpy.array(val, dtype='datetime64[D]')
            else:
                return numpy.array(val)

        plots = []
        for xcol, ycol in zip(self.x_columns, self.y_columns):

            # If a column is given as a tuple (in, out); then use in as the column in the
            # dictionary data, but label it with out in the returned plotting data
            def getinout(col):
                if isinstance(col, tuple):
                    return col[0], col[1]
                return col, col

            xin, xout = getinout(xcol)
            yin, yout = getinout(ycol)

            try:
                # TODO: allow xcol or ycol to be None, and in that case, just make it 0-1-2-3-... numbers
                x, y = zip(*[(row[xin], row[yin]) for row in rows])
            except KeyError as e:
                key = e.args[0]
                raise KeyError(
                    'The output does not contain a column named "{}"'.format(key))

            plot = {
                'x-label': xout,
                'y-label': yout,
                'group': group,
                'x': to_numpy_array(x),
                'y': to_numpy_array(y)
            }

            plots.append(plot)

        return plots


class _PlotFormat(object):

    def __init__(self, dict_format, adaptor):
        self._dict_format = dict_format
        self._adaptor = adaptor

    def update(self, message):
        self._dict_format.update(message)

    def finalize(self):
        dict_output = self._dict_format.finalize()
        return self._finalize(dict_output)

    def _finalize(self, dict_output):
        return self._adaptor.plot(dict_output)


class _DefaultPlotFormat(_PlotFormat):

    def __init__(self, dict_format, plotter, index, columns, group_by):
        super(_DefaultPlotFormat, self).__init__(dict_format, None)
        self._plotter = plotter
        self._index = index
        self._columns = columns
        self._group_by = group_by

    def _finalize(self, dict_output):
        # By default, if not specified in a different way, plot all numeric columns on the y axis
        if self._adaptor is None:
            if dict_output:
                if self._columns is None:
                    self._columns = [column for column, value in six.iteritems(
                        dict_output[0]) if isinstance(value, numbers.Number)]
                self._adaptor = DictPlotAdaptor(
                    self._plotter, self._index, self._columns, self._group_by)
            else:
                raise RuntimeError('No data available to plot')

        return super(_DefaultPlotFormat, self)._finalize(dict_output)


default = object()  # placeholder for default paramater


class _PlotFormatFactory(Format):

    def __init__(self, plotter, get_default_func, index, columns, group_by):
        self._plotter = plotter
        self._get_default_func = get_default_func
        self._index = index
        self._columns = columns
        self._group_by = group_by

    def make_format(self, request_type, *args, **kwargs):
        """Create the format object."""
        index, columns, group_by = self._index, self._columns, self._group_by

        # Resolve default parameters depending on request type
        default_index, default_group_by = self._get_default_func(
            request_type, args, kwargs)
        if index == default:
            index = default_index
        if columns == default:
            columns = None  # determine from numerical columns in DefaultPlotFormat.finalize
        if group_by == default:
            group_by = default_group_by

        dict_fmt = dictionary.make_format(request_type, *args, **kwargs)
        return _DefaultPlotFormat(dict_fmt, self._plotter, index, columns, group_by)


class _PlotFormatFactoryDefault(Format):

    def __init__(self):
        self.default_index_and_groups = {}

    def get_default_index_and_groups(self, request_type, request_args, request_kwargs):
        if request_type not in self.default_index_and_groups:
            return (None, None)
        else:
            return self.default_index_and_groups[request_type](*request_args, **request_kwargs)

    def register_default_index_and_group(self, request_type):
        """Register default index and groups for a certain request type.

        This function is meant to be used as a decorator which registers
        the default index (column to use as x coordinate values) and group(s)
        for a certain request type. The function should return a 2-tuple
        where the first entry specifies the column to be used as x axis coordinates
        and the second entry the column or columns to group by. Both entries
        can be None to have no default x axis column or not create groups
        by default, respectively.
        """
        def decorator(func):
            if request_type in self.default_index_and_groups:
                raise AttributeError(
                    'There is already a default index for requests of type "{}"'.format(request_type))
            self.default_index_and_groups[request_type] = func
            return func
        return decorator

    def __call__(self, plotter, index=default, columns=default, group_by=default):
        """Specify index, columns and groups for the plot."""
        return _PlotFormatFactory(plotter, self.get_default_index_and_groups, index, columns, group_by)


plot = _PlotFormatFactoryDefault()
"""Visualize the request result in some sort of plot.

This formatter can be used to obtain a plot visualizing the resulting data.
This output formatter is independent of the actual plotting backend. Basically,
it formats the data into numpy arrays of x and y values, and hands them to the
backend.

Parameters
----------
    plotter : callable
        A callable which is called with a list of dictionaries. Each dictionary has
        5 values: ``'x-label'``, ``'y-label'`` are strings which describe the X and Y
        values. They can be used by the backend as axis labels, or ignored if axis
        labels are obtained elsewhere. The ``'group'`` item is a tuple with one string
        for each entry in the `group_by` parameter, and it specifies which group this
        data series corresponds to. The ``'x'`` and ``'y'`` items are numpy arrays with
        x and y values. The callable should return an instance representing the
        visualized plot, such as bitmap or vector graphics data.
    index : str, list, None
        Specifies the column(s) to use as x values. If ``None``, a default column is chosen
        depending on the type of request made. For example, the date is used for
        historical end-of-day data requests.
    columns : str, list, None
        Specifies the column(s) to be used as y values. If ``None``, this defaults to a list
        of all numerical columns returned in the request
    group_by : str, list, None
        Lists columns which should be treated as different groups and different plots be
        created. For example, if this is ``'Security'``, a separate plot is created for
        each security in the result. If ``None``, a default choice is made depending
        on the type of request.

An example plotter that comes with `bqapi` is `:class:~bqplot.BQPlotter`.

If both a visualization and the raw numbers of a request are required, the
:attr:`dictionary` output formatter (which is the default) should be used, and
the visualization can then be created with :class:`DictPlotAdaptor`.
"""
