# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

from .plot import plot, default
from .format import Format


class MPLPlotter:

    """A matplotlib-based backend for :attr:`bqapi.plot`.

    An instance of this class can be passed as `plotter` to :attr:`bqapi.plot`.
    It will produce a :class:`matplotlib.axes.Axes` instance.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~MPLPlotter.__init__
        ~MPLPlotter.__call__

    .. automethod:: __init__
    """

    def __init__(self, x_label=None, y_label=None):
        """Constructor for :class:`MPLPlotter` instances.

        Creates a new :class:`MPLPlotter` instance that can be passed to
        :attr:`bqapi.plot`.

        Parameters
        ----------
            x_label : str
                The label to be used for the X axis. If `None`, it will be
                deduced from the data to be plotted.
            y_label : str
                The label to be used for the Y axis. If `None`, it will be
                deduced from the data to be plotted.
        """
        # TODO: Plot options: plot type, scales, colors, ticks, legend location,
        # other kwargs for axes, subplot_kw, figure_kw, etc.
        self._x_label = x_label
        self._y_label = y_label
        self._figure = None
        self._axes = None
        self._plots = {}

    def __call__(self, plots):
        """Create or update the figure with data.

        Creates or updates the matplotlib axes instance with the data
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

    def _create_plot(self, plots):
        import matplotlib.pyplot
        import numpy

        x_label = self._x_label or plots[0]['x-label']
        y_label = self._y_label or plots[0]['y-label']

        subplot_args = {'xlabel': x_label, 'ylabel': y_label}
        self._figure, self._axes = matplotlib.pyplot.subplots(
            subplot_kw=subplot_args)

        x_data, y_data, labels = [list(x) for x in zip(
            *[(plt['x'], plt['y'], ', '.join(list(plt['group']) + [plt['y-label']])) for plt in plots])]
        charts = [self._axes.plot(x, y, label=label)[0]
                  for x, y, label in zip(x_data, y_data, labels)]
        self._axes.legend(loc='best')

        # Make space for date labels
        if isinstance(x_data[0][0], numpy.datetime64):
            # TODO: Show dates always in local time zone
            for label in self._axes.get_xticklabels():
                label.set_ha('right')
                label.set_rotation(35)
            self._figure.subplots_adjust(bottom=0.15)

        # Remember the line instances for future updates
        self._plots = {(plt['x-label'], plt['y-label'], plt['group']): chart for plt, chart in zip(plots, charts)}
        return self._axes

    def _update_plot(self, plots):
        import numpy

        for plt in plots:
            key = plt['x-label'], plt['y-label'], plt['group']
            if key not in self._plots:
                raise Exception('No such plot with x,y,group={}'.format(key))

            chart = self._plots[key]
            line = chart

            # Update the data
            line.set_xdata(numpy.concatenate([line.get_xdata(), plt['x']]))
            line.set_ydata(numpy.concatenate([line.get_ydata(), plt['y']]))

            # TODO: In nbagg, we should only do the following if the user is not panned/zoomed already,
            # but only if the original "home" view is shown. We could check by comparing xlim/ylim
            # to what it was after the last autoscale().

            # Recalculate the bounding area
            self._axes.relim()
            # Set axes limits to bounding area
            self._axes.autoscale()
            # Redraw the plot
            self._figure.canvas.draw()

        return self._axes


class _MPLPlotFormatFactory(Format):

    def __init__(self, index, columns, group_by, x_label, y_label):
        plotter = MPLPlotter(x_label=x_label, y_label=y_label)
        self.factory = plot(plotter, index=index,
                            columns=columns, group_by=group_by)

    def make_format(self, request_type, *args, **kwargs):
        return self.factory.make_format(request_type, *args, **kwargs)


class _MPLPlotFormatFactoryDefault(Format):

    def __call__(self, index=default, columns=default, group_by=default, x_label=None, y_label=None):
        return _MPLPlotFormatFactory(index, columns, group_by, x_label, y_label)

    def make_format(self, request_type, *args, **kwargs):
        return self().make_format(request_type, *args, **kwargs)

    def __repr__(self):
        # Mostly to show something useful in the documentation
        return '{}()'.format(self.__class__.__name__)


mplplot = _MPLPlotFormatFactoryDefault()
"""Return result as a :class:`matplotlib.axes.Axes` instance.

This is an output formatter as described in :ref:`output-formatters`. It can
be used with the `format` parameter for data-retrieving functions such as
:meth:`~bqapi.Session.send_request()` to return the retrieved data as a
matplotlib plot.

Basically, this formatter is simply a shortcut for::

    bqapi.plot(plotter=bqapi.MPLPlotter())

It can be called to fine-tune several parameters of the plot, as well as to
specify what data to put on the X and Y axis. Both the keyword arguments to
:attr:`bqapi.plot` and :meth:`MPLPlotter.__init__` can be specified:

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
"""
