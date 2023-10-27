# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import collections
import functools
import logging
import sys
import os

from bqbreg import bqbreg_eval_int
from bqlmetadata import MetadataItem, MetadataReaderFactory
from bqlmetadata.literals import StringLiteral
from bqutil import trace_it

from .ast import BqapiQueryParser
from .om import BqlItemFactory, Preferences
from .om.python_adaptation import add_formatting, strip_formatting
from .request import (
    Request,
    BqapiRequestExecutor,
    ResponseError
)

_logger = logging.getLogger(__name__)
MAX_RETRIES_ON_ERROR = 3


class _Namespace:
    # Namespace for BQL items.

    # An instance of this class serves as a namespace for a set of items
    # available from a metadata reader.

    def __init__(self, what, metadata_reader):
        self.__metadata_reader = metadata_reader
        self.__what = what

    def __getattr__(self, name):
        name = strip_formatting(name)

        metadata = self.__metadata_reader.get_by_name(self.__what, name)
        if not metadata:
            desc = {
                'function': 'function',
                'data-item': 'data item',
                'universe-handler': 'universe handler'
            }

            csep = ', '.join([desc[x] for x in self.__what[:-1]])
            if not csep:
                final = desc[self.__what[-1]]
            else:
                final = ' or '.join([csep, desc[self.__what[-1]]])

            raise AttributeError(f"No BQL {final} named '{name}'")
        return BqlItemFactory(name, metadata)

    def __dir__(self):
        names = self.__metadata_reader.enumerate_names(self.__what)
        return sorted(set(add_formatting(name) for name in names))


class _DataItemNamespace(_Namespace):
    # BQL data items.

    # Also includes special data items that don't have official BQL metadata.
    def __init__(self, metadata_reader):
        super().__init__(('data-item',), metadata_reader)
        self.__cde_metadata = MetadataItem._create_cde_metadata(
            metadata_reader)
        meta_item = MetadataItem._create_model_mgmt_metadata(metadata_reader)
        self.__model_mgmt_metadata = meta_item
        self.__string_literal = StringLiteral()

    # TODO: Remove these shims once BQL Federated Metadata is available through
    #   bqhopper.
    # Note that we're not creating a _cde or _modmgmt property on
    # _DataItemNamespace and returning a BqlItemFactory like we do for other
    # data items.  The reason is that the BqlItemFactory needs a name, and the
    # name isn't _cde  or _modmgmt like it would be for regular data items,  but
    # rather the name is the mnemonic passed to _cde() and _modmgmt().  Since we
    # don't have a mnemonic yet when the _cde property itself is evaluated, we
    # can't create a BqlItemFactory -- we need to wait until the _cde() or
    # _modmgmt() function is called with all its arguments, and the thing
    # returned by the those functions needs to be a BqlItem.
    def _cde(self, mnemonic, *args, **kwargs):
        """
        Returns custom data from
        `CDE <GO> <https://blinks.bloomberg.com/screens/cde>`_ function
        on the Bloomberg Terminal as a BqlItem.

        You can use :meth:`Service.data._cde` to query
        `CDE <GO> <https://blinks.bloomberg.com/screens/cde>`_
        for custom data.

        For information about uploading custom data in BQuant, see
        the
        `BQCDE How-to Guides </bquant-libraries/custom-data-editor-(bqcde)/bqcde-how-to-guides>`_.

        .. note::

          Because the :meth:`_cde` method belongs to the private
          :class:`_DataItemNamespace` class,
          :class:`_DataItemNamespace` is in the signature for
          :meth:`_cde`. However, in BQuant, you must use the
          :meth:`_cde` method with the :attr:`bql.Service.data` property
          to access custom data. This is because the interface for
          :meth:`_cde` is under development.

        .. admonition:: Prerequisite: Create custom data fields in CDE

            Before you can access custom data, you must create custom
            data fields using the Custom Data Editor
            (`CDE <GO> <https://blinks.bloomberg.com/screens/cde>`_)
            function on the Bloomberg Terminal®. For instructions on
            creating a custom data field, see
            `HELP CDE <GO> <https://blinks.bloomberg.com/screens/help%20cde>`_.

        Parameters
        ----------
        mnemonic : :obj:`str`
            The field mnemonic name for the custom data field you want
            to query. A field mnemonic name is the human-readable
            identifier that you assign to a custom data field when you
            create the field in
            `CDE <GO> <https://blinks.bloomberg.com/screens/cde>`_.
        *args
            Optional positional arguments for defining the date,
            frequency, and fill behavior for custom data.
            The valid parameter groups are ``(start, end, frq, fill)``
            and ``(dates, fill)``. The ``frq`` and ``fill`` arguments
            are optional in the parameter groups. See the keyword
            parameter definitions below for valid values.
        **kwargs
            Additional optional keyword arguments for defining the date,
            frequency, and fill behavior for custom data. The valid
            parameter groups are ``(start, end, frq, fill)`` and
            ``(dates, fill)``. You can exclude the ``frq`` and ``fill``
            arguments in the parameter groups.

            - start : :obj:`str`
                Specifies the start of an inclusive date range for
                custom data. The date must be relative (for example,
                ``-1m``), absolute (in ``YYYY-MM-DD`` format), or
                represented by :meth:`Service.func.today()`. The default
                is ``0d``.
            - end : :obj:`str`
                Specifies the end of an inclusive date range for custom
                data. The date must be relative (for example, ``-1m``),
                absolute (in ``YYYY-MM-DD`` format), or represented by
                :meth:`Service.func.today()`. The default is ``0d``.
            - dates : :obj:`str`
                Returns custom data for an as-of date or a date range.
                The as-of date must be relative (for example, ``-1m``),
                absolute (in ``YYYY-MM-DD`` format), or represented by
                :meth:`Service.func.today()`. You can define the date
                range using :meth:`Service.func.range`.
            - frq : :obj:`str`
                Specifies the sampling frequency for dates in a time
                series defined by the ``dates`` or ``start`` and ``end``
                parameters. Valid values are ``d``, ``w``, ``m``, ``y``,
                ``s``, and ``q``. The default is ``d``.
            - fill : :obj:`str`
                Indicates what value to return when custom data is
                missing. Valid values are ``na`` ``prev`` and ``next``.
                The default is ``na``.

        Returns
        -------
        :class:`bql.BqlItem`
            Custom data from
            `CDE <GO> <https://blinks.bloomberg.com/screens/cde>`_
            represented as a BqlItem data item.

        Example
        -------
        .. admonition:: Prerequisite: Required custom field mnemonics
          for code examples

          To run the code examples below, create a custom field in
          `CDE <GO> <https://blinks.bloomberg.com/screens/cde>`_.
          Name the field ``UD_MODELSTORE_<UUID>`` and replace
          ``<UUID>`` with your Unique User ID (UUID). Set the content
          type to Number.

          Bloomberg recommends using field mnemonic names that are
          appended with your personal Unique User ID (UUID). This is
          because field mnemonic names must be unique for each user in
          a firm. To view your UUID,
          see `IAM <GO> <https://blinks.bloomberg.com/screens/iam>`_.

        Use custom data in a PyBQL request.

        **Input 1**

        .. code-block:: python
            :linenos:

            # Set up environment
            import pandas as pd
            import bqcde
            import bql
            import os

            # Create BQL Service
            bq = bql.Service()

            # Retrieve Unique User ID (UUID) for field mnemonic names
            uuid = os.environ['UUID']

            # List the field mnemonic names for your custom data fields
            list_of_fields = list(bqcde.list_fields())
            for field in list_of_fields:
                print(f'custom field: {field.mnemonic}')

        **Output 1**

        | custom field: UD_MODELSTORE_12345678

        **Input 2**

        .. code-block:: python
            :linenos:
            :emphasize-lines: 8

            universe = 'AAPL US Equity'
            date = '2019-06-11'

            # Assign the field mnemonic name from output 1 to a variable
            modelstore_field = f'UD_MODELSTORE_{uuid}'

            # Retrieve data from a custom data field in CDE
            item = bq.data._cde(modelstore_field, dates=date)

            # Instantiate and execute the Request
            request = bql.Request(universe, item)
            response = bq.execute(request)
            df_modelstore = response[0].df()

            df_modelstore.head()

        **Output 2**

        .. list-table::
            :header-rows: 2

            * -
              - Date
              - UD_MODELSTORE_12345678(dates=2019-06-11)
            * - ID
              -
              -
            * - AAPL US Equity
              - 2019-06-11
              - 0.1908
        """
        return self._shim(mnemonic, self.__cde_metadata, *args, **kwargs)

    def _modmgmt(self, mnemonic, *args, **kwargs):
        return self._shim(mnemonic, self.__model_mgmt_metadata, *args, **kwargs)

    def _shim(self, mnemonic, metadata, *args, **kwargs):
        # Validate the mnemonic as a string.
        mnemonic = self.__string_literal.validate(mnemonic)
        item_factory = BqlItemFactory(mnemonic, [metadata])
        return item_factory(*args, **kwargs)


class Service:
    """The primary entry point for BQL interaction.

    An instance of this class provides an interface for interaction
    with the BQL service. Service class methods allow you to access BQL
    data items and functions, as well as to generate and execute requests.

    Example
    -------
    .. code-block:: python
        :linenos:

        import bql
        bq = bql.Service()

    """

    def __init__(self,
                 metadata_reader=None,
                 request_executor=None,
                 preferences=None,
                 query_parser=None):
        """Construct a new Service object.

        Specify `preferences` if you want certain preferences applied to each
        query request that is submitted through this `Service`.  Note that you
        also have the opportunity to specify preferences through the `Request`
        object or on the items directly via the applyPreferences() function;
        any individual preference settings on the `Request` will take
        precedence over the global preferences specified here.
        """
        self.__bqapi_session = None
        self.__metadata_reader = (metadata_reader or
                                  self.__create_metadata_reader())
        self._request_executor = (request_executor or
                                  self.__create_request_executor())
        self._query_parser = query_parser or self.__create_query_parser()
        self.__preferences = Preferences.create(preferences).validate(
            self.__metadata_reader)
        self.__data = _DataItemNamespace(self.__metadata_reader)
        self.__func = _Namespace(('function',), self.__metadata_reader)
        self.__univ = _Namespace(('universe-handler',), self.__metadata_reader)

    def __get_bqapi_session(self):
        if self.__bqapi_session is None:
            import bqapi
            self.__bqapi_session = bqapi.get_session_singleton()
        return self.__bqapi_session

    def __create_metadata_reader(self):
        factory = MetadataReaderFactory()
        return factory.create_metadata_reader()

    def __create_request_executor(self):
        try:
            from bqle import CisRequestExecutor
            return CisRequestExecutor()
        except ImportError:
            session = self.__get_bqapi_session()
            return BqapiRequestExecutor(session)

    def __create_query_parser(self):
        return BqapiQueryParser(self.__get_bqapi_session())

    @property
    def data(self):
        """An accessor property for all BQL data items.

        You can use this property to access the BQL data items you want
        to use in requests.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 6

            # Setup environment
            import bql
            bq = bql.Service()

            # Instantiate a data item
            data_item = bq.data.px_last()

            # Instantiate and execute Request
            request = bql.Request('AAPL US Equity', data_item)
            response = bq.execute(request)
            data = response[0].df()
            data

        """
        return self.__data

    @property
    def func(self):
        """An accessor property for all BQL functions.

        You can use this method to access BQL functions
        to use in requests.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 6, 8

            # Setup environment
            import bql
            bq = bql.Service()

            # Calculate the 30-day average price
            last_30_days = bq.func.range('-30D', '0D')
            price = bq.data.px_last(dates=last_30_days)
            avg_price = bq.func.avg(price)

            # Instantiate and execute Request
            request = bql.Request('AAPL US Equity', avg_price)
            response = bq.execute(request)
            data = response[0].df()
            data.head()

        """
        return self.__func

    @property
    def univ(self):
        """An accessor property for all BQL universe functions.

        You can use this method to access BQL universe functions
        to use in requests.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 6

            # Setup environment
            import bql
            bq = bql.Service()

            # Instantiate a universe
            universe = bq.univ.members('INDU Index')

            # Instantiate a data item
            data_item = bq.data.px_last()

            # Instantiate and execute Request
            request = bql.Request(universe, data_item)
            response = bq.execute(request)
            data = response[0].df()
            data.head()

        """
        return self.__univ

    @property
    def metadata_reader(self):
        # Return the :class:`MetadataReader` used by the service.
        return self.__metadata_reader

    @property
    def preferences(self):
        """
        Return the default preferences applied to each :class:`Request`
        execution.
        """
        return self.__preferences

    @trace_it(_logger)
    def execute(self, request, callback=None, error_callback=None):
        """Execute the given BQL Request.

        Parameters
        ----------
        request : str or Request
            A BQL query string or :class:`Request` object.

        callback : callable, optional
            A function that runs after the successful execution of a
            :class:`Request`. This function must accept one positional
            argument: a :class:`Response` object. Defaults to ``None``.

            .. warning::
                Passing a ``callback`` function causes this method to return a
                :obj:`bqapi.promise.Promise` rather than a :class:`Response`.
                See the Returns and Examples sections below for more details.

        error_callback : callable, optional
            A function that runs after the unsuccessful execution
            of a Request. This function must accept one positional argument:
            a tuple with three children (the error object, the error message,
            and the traceback object).
            Defaults to None.

        Returns
        -------
        :class:`Response`
            If ``callback`` and ``error_callback`` are both set to ``None``,
            this method returns a :class:`Response` object containing one
            :class:`SingleItemResponse` object for each data item in the
            :class:`Request`.

        bqapi.promise.Promise
            If a ``callback`` function is passed, this method returns a
            :obj:`Promise` object. In this case, you must handle the data
            extraction in the callback function. See the third example below
            for details.

        Examples
        --------
        **Using a Request object**

        .. code-block:: python
            :linenos:
            :emphasize-lines: 10

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

        **Using a query string**

        .. code-block:: python
            :linenos:
            :emphasize-lines: 6

            # Setup environment
            import bql
            bq = bql.Service()

            # Execute a BQL query string
            response = bq.execute("get(ID()) for('AAPL US Equity')")
            data = response[0].df()
            data.head()

        **Using callbacks**

        .. code-block:: python
            :linenos:

            # Setup environment
            import bql
            bq = bql.Service()

            universe = bq.univ.members('INDU Index')
            data_item = bq.data.px_last(fill='prev')

            # When a request completes, add the DataFrame to the dataframes list
            dataframes = []
            def success_cb(response):
                for data_item in response:
                    print(f'{data_item.name} for {str(universe)} completed successfully')
                    # Unpack the data and add it to the list
                    dataframes.append(data_item.df())
                print(dataframes)

            # If a request fails, print the error message
            def error_cb(error):
                error_msg = error[1]
                print(error_msg)

            # Instantiate and execute request
            request = bql.Request(universe, data_item)
            promise = bq.execute(request, callback=success_cb, error_callback=error_cb)

        """
        try:
            if isinstance(request, Request):
                return self._request_executor.execute(
                    request,
                    self.__preferences,
                    callback,
                    error_callback)
            else:
                return self._request_executor.execute_string(
                    request,
                    callback,
                    error_callback)
        except ResponseError as ex:
            # Deliberately re-raise the exception here with a truncated stack
            # trace so that users of the library don't get to see
            # library-internal stack frames when they get an error from the
            # BQL backend.
            raise ex

    @trace_it(_logger)
    def execute_many(self, requests, on_request_error=None, num_retries=0):
        """Execute multiple BQL requests asynchronously.

        Parameters
        ----------
        requests : list
            A list containing one or more BQL :class:`Request` objects or query
            strings.
        on_request_error : callable, optional
            A function that runs after the unsuccessful execution
            of a Request. This function must accept one positional argument:
            a tuple with three children (the error object, the error message,
            and the traceback object). Results of calling error callbacks are
            included in the returned list of results.
            Defaults to None.
        num_retries : int, optional
            The number of times to retry requests before raising an error or
            handling error via `on_request_error`.
            Defaults to 0.

        Returns
        -------
            generator
                Generator of :class:`Response` objects

        Examples
        --------
        **Using Request objects**

        .. code-block:: python
            :linenos:
            :emphasize-lines: 18-19

            # Setup environment
            import bql
            bq = bql.Service()

            price = bq.data.px_last()
            volume = bq.data.px_volume()

            # Instantiate two or more Request objects
            AAPL_request = bql.Request('AAPL US Equity', price)
            MSFT_request = bql.Request('MSFT US Equity', volume)

            # If request fails, print the error message
            def error_cb(error):
                error_msg = error[1]
                print(error_msg)

            # Execute multiple requests with execute_many
            response_generator = bq.execute_many(requests=[AAPL_request, MSFT_request],
                                                 on_request_error=error_cb)
            response_objects = list(response_generator)

            # Unpack the data
            for response in response_objects:
                print(response[0].df())

        |

        **Using query strings**

        .. code-block:: python
            :linenos:
            :emphasize-lines: 9-10, 13

            # Setup environment
            import bql
            bq = bql.Service()

            price = bq.data.px_last()
            volume = bq.data.px_volume()

            # Instantiate two or more query strings
            AAPL_request = "get(PX_LAST()) for(['AAPL US Equity'])"
            MSFT_request = "get(PX_LAST()) for(['MSFT US Equity'])"

            # Execute multiple requests with execute_many
            response_generator = bq.execute_many([AAPL_request, MSFT_request])
            response_objects = list(response_generator)

            # Unpack the data
            for response in response_objects:
                print(response[0].df())

        """
        return self._request_many_async(requests, self.execute,
                                        on_request_error, num_retries)

    def submit(self, request):
        """
        Send a request through the Async BQL service.

        Note that this is different than calling execute() with a callback;
        execute() achieves its nonblocking behavior through the client-side
        API layer, whereas submit() hands off a query to the backend for
        execution and returns immediately with a "job" object.  The caller
        can then call fetch() on the job object to retrieve the response
        when ready.

        Parameters
        ----------
        request : str or :class:`Request`
            A :class:`Request` object or query string.


        Returns
        -------
        :class:`Job` object.
        """
        return self._submit(request, callback=None, error_callback=None)

    def fetch(self, job, is_large_req=False, num_chunks=1):
        """
        Retrieve the response for a submitted BQL request.  The :class:`Job`
        object represents a request that has been send to BQL via the submit()
        function.  fetch() is blocking.

        Parameters
        ----------
        job
            A :class:`Job` object returned by the submit() function.
        is_large_req
            bool: indicating if bql response chunking is turned on.
        num_chunks
            int: number of chunks to be requested at a time in a single
            fetch response.

        Returns
        -------
        :class:`Response` object or raises a :class:`ResponseError` if there
        was an error while executing the submitted BQL request.
        """
        return self._fetch(
            job, callback=None, error_callback=None, is_large_req=is_large_req,
            num_chunks=num_chunks
        )

    def submit_many(self, requests):
        """
        Like submit(), but for submitting more than one request at a time.

        Parameters
        ----------
        requests
            An iterable of :class:`Request` objects or an iterable of query
            strings.

        Returns
        ------
        A generator of :class:`Job` objects, one for each submitted request.
        """
        return self._submit_many(requests)

    def fetch_many(self, jobs, on_request_error=None):
        """
        Retrieve the responses for several submitted requests.

        If on_request_error is None, then a :class:`ResponseError` is raised
        whenever any of the submitted queries fails to execute.  If
        on_request_error is not None, then the on_request_error callback is
        executed on each error reported by the backend.

        Parameters
        ----------
        jobs
            An iterable of :class:'Job` objects, typically returned from the
            submit_many() function.

        on_request_error : callable
            If specified, then this callback is executed for each error that
            is received from the backend when executing the submitted request.
            The return value of the callback is included in the fetch_many()
            many returned list.

        Returns
        -------
        A generator of :class:`Response` objects.
        """
        return self._fetch_many(jobs, on_request_error)

    def submit_fetch_many(self, requests, on_request_error=None):
        """
        Execute several requests through the Async BQL service.

        This is a blocking call and does not return until all the requests
        have been processed.  If on_request_error is specified, then the
        the on_request_error callback is executed for each error reported
        by the backend while executing the requests.

        Parameters
        ----------
        requests
            An iterable of :class:`Request` objects or an iterable of query
            strings.

        on_request_error : callable
            If specified, then this callback is executed for each error that
            is received from the backend when executing the submitted request.
            The return value of the callback is included in the fetch_many()
            many returned list.

        Returns
        -------
        A generator of :class:`Response` objects.
        """
        return self._submit_fetch_many(requests, on_request_error)

    def _parse(self, query_string):
        """
        Parse the given BQL query string and return the corresponding
        bql.Request instance.

        Parameters
        ----------
        query_string
            A BQL query string

        Returns
        -------
        A bql.Request instance
        """
        return self._query_parser.parse_to_om(query_string,
                                              self.metadata_reader)

    @trace_it(_logger)
    def _submit(self, request, callback=None, error_callback=None):
        # Submit request as a 'job'.  The `request` argument can either be a
        # query string or a bql.Request instance.  Returns a Job instance that
        # represents the response from the backend containing a payloadId and
        # in the case the object model was used the data items/context of the
        # request.

        try:
            if isinstance(request, Request):
                return self._request_executor.submit(
                    request,
                    self.__preferences,
                    callback,
                    error_callback)
            else:
                return self._request_executor.submit_string(
                    request,
                    callback,
                    error_callback)
        except ResponseError as ex:
            # Deliberately re-raise the exception here with a truncated stack
            # trace so that users of the library don't get to see
            # library-internal stack frames when they get an error from the
            # BQL backend.
            raise ex

    @trace_it(_logger)
    def _fetch(self, job, callback=None, error_callback=None,
               is_large_req=False, num_chunks=1):
        # Fetch results of a previously submitted/scheduled job.  `job` is
        # the Job object returned from the _submit() call.  Return a Response
        # instance.

        try:
            return self._request_executor.fetch(
                job,
                callback,
                error_callback,
                is_large_req,
                num_chunks
            )

        except ResponseError as ex:
            # Deliberately re-raise the exception here with a truncated stack
            # trace so that users of the library don't get to see
            # library-internal stack frames when they get an error from the
            # BQL backend.
            raise ex

    def _make_retry_callback(self, error_cb, request, num_retries=1):
        if num_retries == 0:
            return error_cb
        elif num_retries > MAX_RETRIES_ON_ERROR:
            raise ValueError(
                f"num_retries must be less than {MAX_RETRIES_ON_ERROR}. "
                f"Got {num_retries}")

        def dummy_callback(resp):
            return resp

        def wrap_callback(n, wrapped_error_callback):
            def new_callback(exc_info):
                retryable_errors = self._request_executor.retryable_exceptions()
                exc_type, exc, exc_tb = exc_info
                if isinstance(exc, retryable_errors):
                    # error callback triggered with a retryable exception
                    _logger.info(
                        "Scheduling retry #%s of request: %s", n, request
                    )
                    return self.execute(request, callback=dummy_callback,
                                        error_callback=wrapped_error_callback)
                else:
                    # callback does what it would have without retry handling
                    return wrapped_error_callback(exc_info)

            return new_callback

        return_callback = error_cb
        for i in range(num_retries):
            return_callback = wrap_callback(num_retries - i, return_callback)

        return return_callback

    def _request_many_async(self, requests, exec_function,
                            on_request_error=None, num_retries=0):
        # Generic function to process multiple requests using a handler
        # (exec_function) that is passed in.  Request can either be a 'query' or
        # of type class::Job containing a payload ID for a previously scheduled
        # query.
        _logger.info("Executing multiple requests")
        if on_request_error is not None:
            _logger.debug(f"Using error callback: {on_request_error}")

        def dummy_callback(x):
            return x

        promises = []

        for request in requests:
            error_callback = self._make_retry_callback(
                on_request_error, request, num_retries=num_retries)

            promise = exec_function(request, callback=dummy_callback,
                                    error_callback=error_callback)
            promises.append(promise)

        _logger.info(f"Made {len(promises)} requests")
        return (promise.result() for promise in promises)

    def _request_many_sync(self, requests, exec_function):
        # Generic function to process multiple requests using a handler
        # (exec_function) that is passed in.  Request can either be a 'query' or
        # of type class::Job containing a payload ID for a previously scheduled
        # query.
        results = []
        for request in requests:
            result = exec_function(request)
            results.append(result)

        return results

    @trace_it(_logger)
    def _submit_many(self, requests, on_request_error=None):
        # Schedule multiple requests at once.
        return self._request_many_async(requests, self._submit,
                                        on_request_error)

    @trace_it(_logger)
    def _fetch_many(self, jobs, on_request_error=None, num_retries=0):
        # Fetch results of multiple scheduled requests at once.
        fetch_func = functools.partial(self._fetch, is_large_req=True)
        return self._request_many_async(jobs, fetch_func,
                                        on_request_error, num_retries)

    @trace_it(_logger)
    def _submit_fetch_many(self, requests, on_request_error=None,
                           num_retries=0):
        def chunk(items, n):
            for i in range(0, len(items), n):
                yield items[i:i+n]

        def dummy_callback(x):
            return x
        fetch_promises = []
        wait_promises = collections.deque()
        for batch in chunk(requests, 16):
            if len(wait_promises) > 0:
                (wait_job, wait_promise) = wait_promises.popleft()
                _logger.info('waiting for payload_id=%s before submitting next '
                             'request', wait_job.payload_id)
                wait_promise.result()
            jobs = list(self._submit_many(batch))
            batch_fetch_promises = [
                self._fetch(job, dummy_callback, on_request_error,
                            is_large_req=True)
                for job in jobs
            ]
            fetch_promises.extend(batch_fetch_promises)
            wait_promises.append((jobs[0], batch_fetch_promises[0]))
        return (p.result() for p in fetch_promises)

    def _with_request_executor(self, request_executor):
        return self.__class__(self.__metadata_reader,
                              request_executor,
                              self.__preferences,
                              self._query_parser)
