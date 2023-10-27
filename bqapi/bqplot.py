# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import datetime

from .plot import plot, default
from .format import Format


class BQPlotter:

    """A BQplot-based backend for :attr:`bqapi.plot`.

    An instance of this class can be passed as `plotter` to :attr:`bqapi.plot`.
    It will produce a :class:`bqplot.Figure` instance.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~BQPlotter.__init__
        ~BQPlotter.__call__

    .. automethod:: __init__
    """

    def __init__(self, x_label=None, y_label=None, x_tick_format=None, y_tick_format=None, legend_location=None):
        """Constructor for :class:`BQPlotter` instances.

        Creates a new :class:`BQPlotter` instance that can be passed to
        :attr:`bqapi.plot`.

        Parameters
        ----------
            x_label : str
                The label to be used for the X axis. If `None`, it will be
                deduced from the data to be plotted.
            y_label : str
                The label to be used for the Y axis. If `None`, it will be
                deduced from the data to be plotted.
            x_tick_format : str
                The format to be used for ticks on the X axis.
            y_tick_format : str
                The format to be used for ticks on the Y axis.
            legend_location : str
                Location of the legend of the figure. One of ``'top-right'``, ``'top'``,
                ``'top-left'``, ``'left'``, ``'bottom-left``', ``'bottom'``, ``'bottom-right'``,
                ``'right'``.
        """
        # TODO: Plot options: plot type, scales, colors
        self._x_label = x_label
        self._y_label = y_label
        self._x_tick_format = x_tick_format
        self._y_tick_format = y_tick_format
        self._legend_location = legend_location
        if legend_location is None:
            self._legend_location = 'top-right'
        self._figure = None
        self._plots = {}

    def __call__(self, plots):
        """Create or update the figure with data.

        Creates or updates the :class:`bqplot.Figure` instance with the data
        represented by `plots`. This is typically called by
        :attr:`bqapi.plot` as soon as the data to be plotted are available.

        Parameters
        ----------
            plots : list
                A list of dictionaries with the plotting data. See
                :attr:`bqapi.plot` for the exact format.
        """
        if self._figure is None:
            return self._create_plot(plots)
        else:
            return self._update_plot(plots)

    def _find_scale_from_data(self, data):
        """Choose an appropriate scale from the data type."""
        from bqplot import DateScale, LinearScale
        import numpy

        if isinstance(data[0], (datetime.date, datetime.time, datetime.datetime, numpy.datetime64)):
            return DateScale()
        else:
            return LinearScale()

    def _create_plot(self, plots):
        """Create a :class:`bqplot.Figure` based on the given data columns."""
        from bqplot import Axis, Lines, Figure

        x_label = self._x_label or plots[0]['x-label']
        y_label = self._y_label or plots[0]['y-label']

        x_scale = self._find_scale_from_data(plots[0]['x'])
        y_scale = self._find_scale_from_data(plots[0]['y'])
        x_data, y_data, labels = [list(x) for x in zip(
            *[(plt['x'], plt['y'], ', '.join(list(plt['group']) + [plt['y-label']])) for plt in plots])]
        x_axis = Axis(scale=x_scale, label=x_label,
                      grid_lines='none', tick_format=self._x_tick_format)
        y_axis = Axis(scale=y_scale, label=y_label, grid_lines='none',
                      tick_format=self._y_tick_format, orientation='vertical')
        # We create one Lines object for each chart separately so that they can have a different number of data points,
        # and we can update them independent from each other.
        from bqplot.marks import CATEGORY10
        default_colors = CATEGORY10
        charts = [Lines(x=x, y=y, scales={'x': x_scale, 'y': y_scale}, labels=[label], display_legend=True,
                        colors=[default_colors[index % len(default_colors)]])
                  for index, (x, y, label) in enumerate(zip(x_data, y_data, labels))]
        self._figure = Figure(
            axes=[x_axis, y_axis], marks=charts, legend_location=self._legend_location)
        # Remember location of groups in data arrays for future updates
        self._plots = {(plt['x-label'], plt['y-label'], plt['group']): index for index, plt in enumerate(plots)}
        return self._figure

    def _update_plot(self, plots):
        """Update the current figure with the given data columns."""
        import numpy

        for plt in plots:
            key = plt['x-label'], plt['y-label'], plt['group']
            if key not in self._plots:
                raise Exception('No such plot with x,y,group={}'.format(key))

            index = self._plots[key]
            chart = self._figure.marks[index]

            chart.x = numpy.concatenate([chart.x, plt['x']])
            chart.y = numpy.concatenate([chart.y, plt['y']])
        return self._figure


class _BQPlotFormatFactory(Format):

    def __init__(self, index, columns, group_by, x_label, y_label, x_tick_format, y_tick_format, legend_location):
        plotter = BQPlotter(x_label=x_label, y_label=y_label, x_tick_format=x_tick_format,
                            y_tick_format=y_tick_format, legend_location=legend_location)
        self._factory = plot(plotter, index=index,
                             columns=columns, group_by=group_by)

    def make_format(self, request_type, *args, **kwargs):
        return self._factory.make_format(request_type, *args, **kwargs)


class _BQPlotFormatFactoryDefault(Format):

    def __call__(self, index=default, columns=default, group_by=default, x_label=None, y_label=None,
                 x_tick_format=None, y_tick_format=None, legend_location=None):
        return _BQPlotFormatFactory(index, columns, group_by, x_label, y_label,
                                    x_tick_format, y_tick_format, legend_location=legend_location)

    def make_format(self, request_type, *args, **kwargs):
        return self().make_format(request_type, *args, **kwargs)

    def __repr__(self):
        # Mostly to show something useful in the documentation
        return '{}()'.format(self.__class__.__name__)


bqplot = _BQPlotFormatFactoryDefault()
"""Return result as a :class:`bqplot.Figure`.

This is an output formatter as described in :ref:`output-formatters`. It can
be used with the `format` parameter for data-retrieving functions such as
:meth:`~bqapi.Session.send_request()` to return the retrieved data as a
:class:`bqplot.Figure` instance.

Basically, this formatter is simply a shortcut for::

    bqapi.plot(plotter=bqapi.BQPlotter())

It can be called to fine-tune several parameters of the plot, as well as to
specify what data to put on the X and Y axis. Both the keyword arguments to
:attr:`bqapi.plot` and :meth:`BQPlotter.__init__` can be specified:

Parameters
----------
    index : list, str
        Specifies the column or columns with the data to use on the X axis.
    columns : list, str
        Specifies the column or columns with the data to use on the Y axis.
    group_by : list, str
        Specifies columns to group group individiual plots by. For example,
        if historical data is obtained for multiple securities, then
        specifying the ``'Security'`` column as a group would create individual
        plots for each security.
    x_label : str
        The label to be used for the X axis. If `None`, it will be
        deduced from the data to be plotted.
    y_label : str
        The label to be used for the Y axis. If `None`, it will be
        deduced from the data to be plotted.
    x_tick_format : str
        The format to be used for ticks on the X axis.
    y_tick_format : str
        The format to be used for ticks on the Y axis.
    legend_location : str
        Location of the legend of the figure. One of ``'top-right'``, ``'top'``,
        ``'top-left'``, ``'left'``, ``'bottom-left``', ``'bottom'``, ``'bottom-right'``,
        ``'right'``.
"""
