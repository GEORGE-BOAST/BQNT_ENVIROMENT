# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import collections
import logging
import functools
import six
import sys

import blpapi

from .request import AuthorizationRequest, GenericRequest, Subscription
from .utils import tz_from_name
from .error import SessionClosedError, ServiceClosedError, AuthorizationError
from .promise import Promise
from .event_loop import get_default_event_loop
from .event_thread import EventThread
from .settings import Settings
from .element import Event, fill_element
from .format import no_format, make_format, SimpleFormat
from .tzdf import get_tzdf
from .shim import shim_warning


class Session:

    """Primary entry point for data queries.

    An instance of the `Session` class represents a session with the
    Bloomberg data service. It can be used to request data and
    subscribe to real-time data streams.

    Most functionality is available with both synchronous (blocking)
    and asynchronous (non-blocking) function calls. Functions are
    usually blocking by default, unless either the `callback` or
    `error_callback` parameters are set to a callable which will be
    called when the operation finishes. In that case, the function
    returns a :class:`Promise` object for the result, whereas the
    synchronous versions directly return the result. The :attr:`async`
    attribute can be used to obtain a promise for the raw values
    asynchronously, without the need to specify callbacks.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~Session.__init__
        ~Session.close
        ~Session.is_open
        ~Session.make_format
        ~Session.promise_identity
        ~Session.promise_service
        ~Session.promise_tzdf
        ~Session.register
        ~Session.register_callback
        ~Session.send_request
        ~Session.send_subscribe
        ~Session.unsubscribe

    .. automethod:: Session.__init__

    """

    def __init__(self, event_loop=None, **settings):
        """Constructor for :class:`Session` instances.

        After constructing a Session object, the session is automatically started
        asynchronously, i.e. the constructor returns immediately. Any call to
        data-retrieving functions will wait for the session to actually open and
        potentially authenticate before sending any request.

        Parameters
        ----------
        event_loop : :class:`EventLoop`, ``None``
            An event loop used to post and retrieve the events from
            the Bloomberg data service. Can be ``None`` to use the default
            built-in event loop.

            This can be any object that has methods called `call_later()`
            and `call_soon_threadsafe()`. In order to make synchronous calls,
            the event loop must also support `run_forever()` and `stop()`
            calls. In particular, :class:`asyncio.BaseEventLoop` instances
            can be used as an argument, and in case you want to run this with
            a :class:`tornado.ioloop.IOLoop` instance, the :class:`TornadoEventLoop`
            adaptor can be used to make the interface compatible.
        settings : dict
            Session settings. See the :class:`Settings` class for more details.
        """
        self.event_loop = event_loop or get_default_event_loop()

        self._settings = Settings(**settings)
        self._logger = logging.getLogger('bqapi.Session')
        self._session = None

        # Initially open the session
        self._open()

    def _open(self):
        assert self._session is None

        options = blpapi.SessionOptions()
        for index, (hostname, port) in enumerate(self._settings.addresses):
            options.setServerAddress(
                str(hostname), port, index)  # py2 workaround
        options.setAuthenticationOptions(
            str(self._settings._get_auth_string()))  # py2 workaround
        options.setNumStartAttempts(self._settings.numStartAttempts)

        if self._settings.tls_options:
            try:
                # Abbreviation
                tls_input = self._settings.tls_options

                # Branch based on the tls input type
                if isinstance(tls_input, blpapi.sessionoptions.TlsOptions):
                    # blpapi TlsOptions
                    options.setTlsOptions(tls_input)
                elif isinstance(tls_input, collections.abc.Mapping):
                    # dict-like
                    tls_client_cred_path = tls_input['client_cred_path']
                    tls_client_cred_pass = tls_input['client_cred_pass']
                    tls_trusted_certs_path = tls_input['trusted_certs_path']
                    tls_options = blpapi.TlsOptions.createFromFiles(
                        tls_client_cred_path,
                        tls_client_cred_pass,
                        tls_trusted_certs_path)
                    options.setTlsOptions(tls_options)
                else:
                    # unsupported
                    raise TypeError(
                        f'Unsupported tls_options type: {type(tls_input)}.')
            except KeyError as e:
                raise KeyError('Missing field in tls_options', e)

        self._session = blpapi.Session(options)

        self._event_thread = EventThread(
            self._session, self.event_loop, self._on_event)
        self._event_thread.start()

        # Session-wide callbacks
        self._close_promise = None
        self._broadcast_callbacks = {}

        # Mapping from service name to service object, or promise to be resolved into a service object
        self._services = {}
        # Authorized Identity
        self._got_identity = False
        self._identity = None
        # Mapping from correlation ID to request objects
        self._correlation_ids = {}
        # Simple counter for generating unique correlation IDs
        self._correlation_id = 1

        # Start the session
        self._start_promise = Promise(self.event_loop)
        self._logger.debug('Starting session...')
        self._session.startAsync()

        self._tz_promise = None

    def is_open(self):
        """Return whether the session is open.

        Only if the session is open, data can be requested or subscriptions
        can be made. When a session is newly created it is initially open,
        and is closed when :meth:`close()` is called, or the session is
        terminated asynchronously, for example when no connection to the
        data service can be established or the connection gets dropped.
        """
        # A session that is being opened is considered open, since we allow
        # data requests to be made. Those data requests simply wait until
        # the session is open before proceeding.
        if self._session is not None:
            return True
        return False

    def close(self, callback=None, error_callback=None):
        """Close the session if it is still open.

        The session is closed asynchronously, ``None`` is returned when the session
        has been closed. When the session has been closed, it is guaranteed that no
        more events are processed. All pending promises are guaranteed to have
        been finished or failed.

        Raise :class:`SessionClosedError` if the session is closed already.

        Parameters
        ----------
            callback : callable
                If given, the function will be executed asynchronously and
                this callable is called with its returned value as soon as
                the asynchronous operation finishes successfully. In this case,
                a :class:`Promise` will be returned which will yield the return
                value of this callable.
            error_callback : callable
                Same as `callback`, but it will be called with a ``(type, value, traceback)``-tuple
                when the asynchronous operation fails.
        """
        if self._session is None or self._close_promise is not None:
            raise SessionClosedError('Session is closed already')

        self._close_promise = Promise(self.event_loop)

        self._logger.debug('Stopping session...')
        self._session.stopAsync()
        return self._close_promise.sync_then(callback, error_callback)

    @property
    def settings(self):
        """The settings this session has been constructed with."""
        return self._settings

    def register_callback(self, event, callback):
        """Register a callback for asynchronous events.

        This function registers a callback which is invoked when the given
        event occurs. Events are identified by a string, and the following
        events are defined:

            * SessionStarted:
                When the session has been started and requests
                can be sent. Note that usually this is not needed
                because requests can already be made before the
                session actually started, and they will be sent
                to the data service as soon as the session has
                started. The callback takes the session object
                as its only argument.
            * SessionTerminated:
                When the session has been terminated, either
                after a call to :meth:`close()`, or asynchronously
                when the connection to the data service gets dropped. The callback takes
                the session object and a potentially-``None``
                exc_info tuple which is filled if the session
                is terminated in response to an error.

        Parameters
        ----------
        event : str
            A string identifying the event type for which to register
            the callback.
        callback : callable
            The function to call when the given event occurs.
        """
        # TODO: There are actually other event types, but they are not
        # documented here because they are not useful typically, and might
        # be removed in the future. The above listed callbacks might be
        # replaced with promises.

        # `SessionStarted(session)`
        # `SessionTerminated(session, exc_info)`
        # `ServiceOpened(session, service_name, blpapi_service)`
        # `SubscriptionStarted(session, subscription)`
        # `SubscriptionTerminated(session, subscription)`
        # `MarketDataEvents(session, security, fields, values)`

        # TODO: Add "Authorized" callback?
        self._broadcast_callbacks.setdefault(event, []).append(callback)

    def make_format(self, format, format_args, request_type, *args, **kwargs):
        """Return an output formatter instance for the given request type.

        This function takes a formatter factory and instantiates a
        formatter instance for the given request type. See :ref:`output-formatters`
        for more details on formatters.

        Parameters
        ----------
            format : object, ``None``
                A formatter factory or formatter instance. If this is a formatter instance,
                the remaining parameters are ignored, and `format` is returned. Otherwise,
                the `make_format()` method is called on this object to create a formatter
                instance with the remaining parameters. If ``None``, the value for this
                parameter is taken from the session's :class:`Settings`.
            format_args : dict, ``None``
                If this is not ``None`` or empty, `format` will be called with these
                arguments before creating the output formatter.
            request_type : str
                The request type for which to create an output formatter.
            args : list
                Positional parameters to instantiate the output formatter with.
            kwargs : dict
                Keyword arguments to instantiate the output formatter with.
        """
        format = format or self._settings.format
        return make_format(format, format_args, request_type, *args, **kwargs)

    def promise_service(self, service_name):
        """Return a promise for the service with the given name.

        This function attempts to open the service with the given name
        if it is not open yet, and returns a promise that will resolve
        to the :class:`blpapi.Service` instance when it has been opened.
        If the service has been opened before, it returns a promise that
        is resolved already.

        Parameters
        ----------
            service_name : str
                The name of the service to open, such as ``'//blp/refdata'``.
        """
        def session_started(session):
            if service_name not in self._services:
                self._logger.debug('Opening Service "%s"...', service_name)
                self._session.openServiceAsync(service_name)
                self._services[service_name] = promise = Promise(
                    self.event_loop)

            if isinstance(self._services[service_name], Promise):
                promise = self._services[service_name]
            else:
                promise = Promise(self.event_loop)
                promise.set_result(self._services[service_name])

            return promise

        # The session might have gone down e.g. when bbcomm got restarted.
        # Attempt to re-connect in that case.
        if not self.is_open():
            self._open()

        if not self._start_promise.done():
            return self._start_promise.then(session_started)
        else:
            return session_started(self._start_promise.result())

    def promise_identity(self):
        """Authorize and return a promise for the authorized identity.

        This function attempts to authenticate with the authentication
        options in the session's :class:`Settings`, and produce an
        authorized :class:`blpapi.Identity` instance. If such an identity
        exists already, from an ealier call to this function, a promise
        that is resolved already is returned.
        """
        def session_started(session):
            if self._identity is None and not self._got_identity:
                if self._settings.auth_type is not None:
                    self._session.generateToken()
                    self._identity = Promise(self.event_loop)
                elif self._settings.authorization:
                    self._identity = self.promise_service('//blp/apiauth')\
                        .then(functools.partial(self._send_auth_request, self._settings.authorization))
                else:
                    self._got_identity = True

            if isinstance(self._identity, Promise):
                promise = self._identity
            else:
                promise = Promise(self.event_loop)
                promise.set_result(self._identity)

            return promise

        # The session might have gone down e.g. when bbcomm got restarted.
        # Attempt to re-connect in that case.
        if not self.is_open():
            self._open()

        if not self._start_promise.done():
            return self._start_promise.then(session_started)
        else:
            return session_started(self._start_promise.result())

    def promise_tzdf(self):
        """Return a promise for the TZDF timezone.

        Return a promise that resolves to a :class:`datetime.tzinfo` object
        representing the timezone the user has selected on ``TZDF<Go>``.
        """
        if self._tz_promise is None:
            self._tz_promise = get_tzdf(self)
        return self._tz_promise

    def send_request(self, service, request_name, request_data,
                     format=None, source_time_zone=None, dest_time_zone=None,
                     identity=None, callback=None, error_callback=None):
        """Send a request to a Bloomberg data service.

        This function sends an arbitrary request to a Bloomberg data
        service. The function returns when all partial responses and the final
        response for the request have been received. It returns a list of
        messages, represented by :class:`MessageElement` instances, unless a
        different handler is specified with the `format` parameter.

        Parameters
        ----------
            service : str, :class:`Promise`
                The service name to send the request to, e.g. ``'//blp/refdata'``,
                or a :class:`Promise` resulting in a :class:`blpapi.Service` object,
                such as obtained from :meth:`promise_service()`.
            request_name : str
                The type of request to send, e.g. ``'ReferenceDataRequest'.``
            request_data : dict
                A dictionary with parameters for the request. The dictionary
                values can again be lists or dictionaries, but the
                structure must comply to the schema for this type of request.
                An example would be
                ``{'securities': ['ibm us equity'], 'fields': ['px last']}``
                for a reference data request.
            format : object
                A formatter which specifies in which format to return tha data.
                This should be a formatter *instance*, not the formatter factory.
                See :ref:`output-formatters` for more details on formatters. Uses a default
                formatter if `format` is None.
            source_time_zone : `str`, :class:`datetime.tzinfo`, ``None``
                The time zone in which to interpret all :class:`datetime.time` or
                :class:`datetime.datetime` occurences in the response of the request
                that are timezone-naive. The special value
                ``'tzdf'`` refers to the time zone specified in ``TZDF<Go>`` on
                the Bloomberg terminal. If ``None``, times are always returned
                without time zone.
            dest_time_zone : `str`, :class:`datetime.tzinfo`, ``None``
                The time zone in which to return all :class:`datetime.time` or
                :class:`datetime.datetime` instances in the response of the request. If ``None``,
                the default time zone set in the session's :class:`Settings` object is used.
                Ignored if `source_time_zone` is ``None``.
            identity : :class:`Promise`, ``None``
                A promise that results in a :class:`blpapi.Identity` instance or ``None``,
                in which case :meth:`promise_identity()` will be called.
            callback : callable
                If given, the function will be executed asynchronously and
                this callable is called with its returned value as soon as
                the asynchronous operation finishes successfully. In this case,
                a :class:`Promise` will be returned which will yield the return
                value of this callable.
            error_callback : callable
                Same as `callback`, but it will be called with a ``(type, value, traceback)``-tuple
                when the asynchronous operation fails.
        """

        # TODO: If we allowed promises anywhere in request_data, it would make
        # client implementations easier.
        # TODO: Then we could also skip the identity parameter

        if isinstance(service, six.string_types):
            service = self.promise_service(service)

        if identity is None:
            identity = self.promise_identity()

        # if format is None, use a new SimpleFormat instance
        # format objects can't be shared among requests because they store
        if format is no_format:
            self._logger.warning(
                'Using the no_format object is deprecated. Please set '
                'format=None in the call to send_request, or supply a new '
                'SimpleFormat instance')
            format = SimpleFormat()

        format = format or SimpleFormat()

        # Obtain TZDF time zone if needed
        if self._tz_promise is None and ((isinstance(source_time_zone, six.string_types) and source_time_zone.lower() == 'tzdf')
                                         or (isinstance(dest_time_zone, six.string_types) and dest_time_zone.lower() == 'tzdf')):
            self._tz_promise = get_tzdf(self)

        def with_service_and_identity(service_and_identity):
            service, identity = service_and_identity
            request = service.createRequest(request_name)
            correlation_id = self._generate_correlation_id()
            fill_element(request.asElement(), request_data)

            self._session.sendRequest(
                request, identity=identity, correlationId=blpapi.CorrelationId(correlation_id))
            self._logger.debug('Sent request of type "%s" to service "%s" with correlation ID %d',
                               request_name, str(service.name()), correlation_id)

            promise = Promise(self.event_loop)
            self._correlation_ids[correlation_id] = GenericRequest(
                promise, format)
            self._correlation_ids[correlation_id].stz = source_time_zone
            self._correlation_ids[correlation_id].dtz = dest_time_zone
            return promise

        return Promise.when(service, identity).then(with_service_and_identity).sync_then(callback, error_callback)

    # TODO: Allow multiple topics in one go, but it is a bit tricky since we get separate replies for each
    def send_subscribe(self, service, topic, fields=None, options=None,
                       format=None, source_time_zone=None, dest_time_zone=None, identity=None):
        """Subscribe to a topic with a Bloomberg data service.

        Subscribes to a topic with a Bloomberg service. Returns a
        :class:`Subscription` instance which provides three :class:`Promise`
        instances: :meth:`Subscription.on_update()`,
        :meth:`Subscription.on_subscribe()`, and
        :meth:`Subscription.on_unsubscribe()`.  They can be used to be notified
        of subscription ticks, establishment, or cancellation of the
        subscription. The :meth:`Subscription.on_update()` promise is a
        multi-promise, i.e. it can deliver values repeatedly. It will deliver a
        value for each tick sent by the service as a :class:`MessageElement`
        instance.

        Parameters
        ----------
            service : str, :class:`Promise`
                The service to which to subscribe, e.g. ``'//blp/mktdata'``,
                or a :class:`Promise` resulting in a :class:`blpapi.Service` object,
                such as obtained from :meth:`promise_service()`.
            topic : str, :class:`Promise`, ``None``
                The topic string of the subscription, or ``None``. For example, this
                can be a security to receive real-time market data for.
            fields : str, list
                The fields to subscribe to, or ``None``.
            options : dict
                Additional subscription options, or ``None``.
            format : object
                A formatter which specifies in which format to return tha data.
                This should be a formatter *instance*, not the formatter factory.
                See :ref:`output-formatters` for more details on formatters.
                If `None`, use a :attr:`no_format` instance.
            source_time_zone : `str`, :class:`datetime.tzinfo`, ``None``
                The time zone in which to interpret all :class:`datetime.time` or
                :class:`datetime.datetime` occurences in the response of the request
                that do not specify a timezone on their own. The special value
                ``'tzdf'`` refers to the time zone specified in ``TZDF<Go>`` on
                the Bloomberg terminal. If ``None``, times are always returned
                without time zone.
            dest_time_zone : `str`, :class:`datetime.tzinfo`, ``None``
                The time zone in which to return all :class:`datetime.time` or
                :class:`datetime.datetime` instances in the response of the request. If ``None``,
                the default time zone set in the session's :class:`Settings` object is used.
                Ignored if `source_time_zone` is ``None``.
            identity : :class:`Promise`, ``None``
                A promise that results in a :class:`blpapi.Identity` instance or ``None``,
                in which case :meth:`promise_identity()` will be called.
        """

        # TODO: Allow fields and/or options to be promises.

        if isinstance(service, six.string_types):
            service = self.promise_service(service)

        if identity is None:
            identity = self.promise_identity()

        # Obtain TZDF time zone if needed
        if self._tz_promise is None and ((isinstance(source_time_zone, six.string_types) and source_time_zone.lower() == 'tzdf')
                                         or (isinstance(dest_time_zone, six.string_types) and dest_time_zone.lower() == 'tzdf')):
            self._tz_promise = get_tzdf(self)

        if not isinstance(topic, Promise):
            topic_promise = Promise(self.event_loop)
            topic_promise.set_result(topic)
            topic = topic_promise

        # note: must have a separate format instance per request
        format = format or SimpleFormat()

        correlation_id = self._generate_correlation_id()
        subscription = Subscription(correlation_id, self.event_loop)

        def with_service_and_identity(service_identity_topic):
            service_instance, identity_instance, topic = service_identity_topic
            try:
                subscription.set_format_and_topic(format, topic)

                subscription_list = blpapi.SubscriptionList()
                correlation_id = subscription.correlation_id
                # options = '&'.join(['='.join([key, value]) for key, value in six.iteritems(options)])
                # TODO: Interpret timezones
                subscription_list.add(str(service_instance.name(
                )) + '/' + topic, fields, options, blpapi.CorrelationId(correlation_id))

                self._correlation_ids[correlation_id] = subscription
                self._correlation_ids[correlation_id].stz = source_time_zone
                self._correlation_ids[correlation_id].dtz = dest_time_zone

                self._logger.debug('Subscribing to "%s" with correlation ID %d...',
                                   subscription_list.topicStringAt(0), correlation_id)
                self._session.subscribe(
                    subscription_list, identity=identity_instance)
            except Exception:  # should not happen, but let's be careful
                exc_info = sys.exc_info()
                subscription._set_exc_info(exc_info)

        def service_or_identity_error(exc_info):
            # If we could not get the service or the identity, propagate the error
            # to all subscription promises.
            subscription._set_exc_info(exc_info)

        Promise.when(service, identity, topic).then(
            with_service_and_identity, service_or_identity_error)
        return subscription

    def unsubscribe(self, subscription, callback=None, error_callback=None):
        """Unsubscribe from a topic.

        Unsubscribe from a topic such that no more real-time updates are retrieved.
        The function returns once the unsubscription has been processed and acknowledged by
        the server. Once it returns, it is guaranteed that no more subscription updates
        are received.

        Parameters
        ----------
            subscription : :class:`Subscription`
                A `Subscription` instance as returned by :meth:`send_subscribe()`.
            callback : callable
                If given, the function will be executed asynchronously and
                this callable is called with its returned value as soon as
                the asynchronous operation finishes successfully. In this case,
                a :class:`Promise` will be returned which will yield the return
                value of this callable.
            error_callback : callable
                Same as `callback`, but it will be called with a ``(type, value, traceback)``-tuple
                when the asynchronous operation fails.
        """
        if not self.is_open():
            raise SessionClosedError('Session is closed')

        promise = subscription.on_unsubscribe()

        subscriptions = blpapi.SubscriptionList()
        subscriptions.add(None, correlationId=blpapi.CorrelationId(
            subscription.correlation_id))

        self._logger.debug(
            'Unsubscribing from subscription with correlation ID %d...', subscription.correlation_id)
        self._session.unsubscribe(subscriptions)

        promise.sync_then(callback, error_callback)

    @classmethod
    def register(cls, func):
        """Register a custom data-retrieving function with the :class:`Session` class.

        This is a decorator that can be used by implementations of functions which
        use the low-level :meth:`send_request()` or :meth:`send_subscribe()` functionality
        to abstract away the details of a specific service. The decorator makes the function
        available as an instance method of the :class:`Session` class. The first parameter
        of the decorated function must be a :class:`Session` instance. Below is an example
        for a function making a request to an imaginary service::

            @bqapi.Session.register
            def get_some_data(session, parameters, format=None, callback=None, error_callback=None):
                session.send_request('//blp/someservice', 'GetSomething', {'Parameters': parameters},
                                     session.make_format(format, 'SomeService.GetSomething'),
                                     callback=callback, error_callback=error_callback)
        """
        if hasattr(cls, func.__name__):
            raise AttributeError(
                'Attribute with name "{}" exists already'.format(func.__name__))
        setattr(cls, func.__name__, func)
        return func

    def _generate_correlation_id(self):
        """Creates a new, unique correlation ID."""
        id = self._correlation_id
        assert id not in self._correlation_ids
        self._correlation_id += 1
        return id

    def _invoke_callback(self, callback, *args, **kwargs):
        """Invokes a callback such that no exceptions propagate to the caller."""
        # Ignore exceptions that would fall through a session callback,
        # to make sure we process all registered callbacks, and so
        # that the session object stays in a consistent state when
        # there is important code to run after the callback is called.
        try:
            callback(*args, **kwargs)
        except:
            self._logger.exception('Unhandled exception from session callback')

    def _broadcast(self, event_type, *args, **kwargs):
        """Invoke all callbacks registered for the given event."""
        for cb in self._broadcast_callbacks.get(event_type, []):
            self._invoke_callback(cb, self, *args, **kwargs)

    def _on_event(self, ev):
        """Primary event handler."""

        event_type, event = ev.eventType(), Event(ev)

        if self._logger.isEnabledFor(logging.INFO):
            event_names = {
                blpapi.Event.ADMIN: 'ADMIN',
                blpapi.Event.AUTHORIZATION_STATUS: 'AUTHORIZATION_STATUS',
                blpapi.Event.PARTIAL_RESPONSE: 'PARTIAL_RESPONSE',
                blpapi.Event.REQUEST: 'REQUEST',
                blpapi.Event.REQUEST_STATUS: 'REQUEST_STATUS',
                blpapi.Event.RESOLUTION_STATUS: 'RESOLUTION_STATUS',
                blpapi.Event.RESPONSE: 'RESPONSE',
                blpapi.Event.SERVICE_STATUS: 'SERVICE_STATUS',
                blpapi.Event.SESSION_STATUS: 'SESSION_STATUS',
                blpapi.Event.SUBSCRIPTION_DATA: 'SUBSCRIPTION_DATA',
                blpapi.Event.SUBSCRIPTION_STATUS: 'SUBSCRIPTION_STATUS',
                blpapi.Event.TIMEOUT: 'TIMEOUT',
                blpapi.Event.TOKEN_STATUS: 'TOKEN_STATUS',
                blpapi.Event.TOPIC_STATUS: 'TOPIC_STATUS',
                blpapi.Event.UNKNOWN: 'UNKNOWN'
            }

            messages = ['type "{}" (correlation_id={})'.format(msg.message_type(),
                                                               ', '.join([str(x) for x in msg.correlation_ids()]))
                        for msg in event]
            self._logger.debug('Got event of type "%s" (%d) with messages\n  %s', event_names.get(
                event_type, 'UNKNOWN'), event_type, '\n  '.join(messages))

        if event_type == blpapi.Event.SESSION_STATUS:
            self._handle_session_status(event)
        elif event_type == blpapi.Event.SERVICE_STATUS:
            self._handle_service_status(event)
        elif event_type == blpapi.Event.TOKEN_STATUS:
            self._handle_token_status(event)
        else:
            self._handle_correlation_event(event_type, event)

    def _handle_session_status(self, event):
        """Handle an event of type SESSION_STATUS."""
        try:
            for message in event:
                if message.message_type() == 'SessionStarted':
                    assert self._start_promise is not None and not self._start_promise.done()
                    self._start_promise.set_result(self)
                    self._broadcast('SessionStarted')
                elif message.message_type() == 'SessionStartupFailure':
                    reason = message['reason']['description']
                    raise SessionClosedError(
                        'Failed to start session: {}'.format(reason))
                elif message.message_type() == 'SessionTerminated':
                    if self._close_promise:
                        self._close_promise.set_result(None)
                    # TODO: Can we get a reason here?
                    raise SessionClosedError('Session terminated')
        except Exception:
            exc_info = sys.exc_info()
            self._session = None
            for id, obj in six.iteritems(self._correlation_ids):
                obj.handle_session_terminated(self, exc_info)
            if not self._start_promise.done():
                self._start_promise.set_exc_info(exc_info)
            if self._close_promise is not None and not self._close_promise.done():
                self._close_promise.set_exc_info(exc_info)
            self._broadcast('SessionTerminated', exc_info)

    def _handle_service_status(self, event):
        """Handle an event of type SERVICE_STATUS."""

        # FIXME: When connecting to bpipe instead of bbcomm, the client receives
        # an unxpected SERVICE_STATUS event with no service name. As a
        # workaround, ignore all SERVICE_STATUS till the service promise hasnt
        # complete
        if not self._start_promise.done():
            self._logger.debug('Ignoring service status event')
            return

        for message in event:
            # TODO: Handle if the service is closed asynchronously, by cancelling all outstanding requests
            # and subscriptions related to that service.
            if message.message_type() in ['ServiceOpened', 'ServiceOpenFailure']:
                service_name = message['serviceName']

                # Services should not be opened without us requisting it
                # TODO: But we should maybe not fail if they do
                assert service_name in self._services
                assert isinstance(self._services[service_name], Promise)
                callback_promise = self._services[service_name]

                try:
                    if message.message_type() == 'ServiceOpenFailure':
                        del self._services[service_name]
                        reason = message['reason']['description']
                        raise ServiceClosedError(
                            'Failed to open service: {}'.format(reason))
                    else:
                        service = self._session.getService(service_name)
                        self._services[service_name] = service
                        callback_promise.set_result(service)
                        self._broadcast('ServiceOpened', service_name, service)
                except ServiceClosedError:
                    exc_info = sys.exc_info()
                    callback_promise.set_exc_info(exc_info)

    def _handle_token_status(self, event):
        """Handle an event of type TOKEN_STATUS."""
        # In this function, the not self._got_identity checks should never fail
        # in principle. However, they are not assertions so that we do not get confused by
        # the server sending bogus TOKEN_STATUS messages.

        def auth_finished(identity):
            if not self._got_identity:
                self._got_identity = True
                promise, self._identity = self._identity, identity
                promise.set_result(identity)

        def auth_failed(exc_info):
            if not self._got_identity:
                self._got_identity = True  # Don't try again to authorize with the same credentials

                promise, self._identity = self._identity, None
                promise.set_exc_info(exc_info)

        for message in event:
            try:
                if message.message_type() == 'TokenGenerationSuccess':
                    # When we have the token, get the auth service and send the auth request
                    token = message['token']
                    self.promise_service('//blp/apiauth')\
                        .then(functools.partial(self._send_auth_request, {'token': token}))\
                        .then(auth_finished, auth_failed)
                elif message.message_type() == 'TokenGenerationFailure':
                    raise AuthorizationError('Failed to generate token: {}'.format(
                        message['reason']['description']))
            except AuthorizationError:
                exc_info = sys.exc_info()
                if not self._got_identity:
                    self._identity.set_exc_info(exc_info)

    def _handle_correlation_event(self, event_type, event):
        """Handle an event for which we have registered its correlation ID and propagate all messages to the request handler."""
        # First, sort messages by their correlation ID, so that we
        # can decide whether a given message is the last one for
        # the same correlation ID.
        self._logger.debug('Event of type {}: {}'.format(event_type, event))
        handler_calls = {}
        for message in event:
            for correlation_id in message.correlation_ids():
                # Do the raw "message received" callbacks
                self._broadcast(
                    "Raw" + str(message.message_type()), correlation_id, message)

                # The server should not send us a correlation ID that we have not requested;
                # but if it happens, we just ignore the response. This might also happen if
                # a request was performed directly with send_request().
                if correlation_id not in self._correlation_ids:
                    continue

                handler_calls.setdefault(correlation_id, []).append(message)

        # Then, go through that list, and make all callbacks
        for correlation_id, message_list in six.iteritems(handler_calls):
            bprequest = self._correlation_ids[correlation_id]
            new_request = bprequest

            try:
                if self._tz_promise is None or not self._tz_promise.done():
                    # We don't have TZDF yet: Wait for it if we need it, otherwise just go ahead
                    tzdf = next((x for x in [bprequest.stz, bprequest.dtz] if isinstance(
                        x, six.string_types) and x.lower() == 'tzdf'), None)
                    if tzdf is not None:
                        assert self._tz_promise is not None
                        self._tz_promise.then(functools.partial(self._handle_tzdf,
                                                                correlation_id,
                                                                bprequest,
                                                                event_type,
                                                                message_list), functools.partial(
                            self._handle_tzdf_error, correlation_id, bprequest))
                    else:
                        new_request = self._handle_message_list(
                            bprequest, event_type, message_list, None)
                else:
                    new_request = self._handle_message_list(
                        bprequest, event_type, message_list, self._tz_promise.result())
            except:
                exc_info = sys.exc_info()
                bprequest._set_exc_info(exc_info)
                new_request = None
            finally:
                self._update_handler(correlation_id, bprequest, new_request)

    def _handle_message_list(self, bprequest, event_type, message_list, tzdf):
        self._logger.debug("In _handle_message_list bprequest=%s, event_type=%s, message_list=%s, tzdf=%s",
                           bprequest, event_type, message_list, tzdf)
        stz = None
        if bprequest.stz is not None:
            stz = tz_from_name(bprequest.stz, tzdf)
        dtz = tz_from_name(bprequest.dtz or self._settings.time_zone, tzdf)

        handler_method = {
            blpapi.Event.PARTIAL_RESPONSE: bprequest.handle_partial_response,
            blpapi.Event.RESPONSE: bprequest.handle_response,
            blpapi.Event.REQUEST_STATUS: bprequest.handle_request_status,
            blpapi.Event.SUBSCRIPTION_STATUS: bprequest.handle_subscription_status,
            blpapi.Event.SUBSCRIPTION_DATA: bprequest.handle_subscription_data,
        }

        update = bprequest
        for index, message in enumerate(message_list):
            message.stz = stz
            message.dtz = dtz
            if event_type in handler_method:
                last_message = (index == len(message_list) - 1)
                self._logger.debug("last_message=%s index=%s, message_list=%s, bprequest=%s",
                                   last_message, index, message_list, bprequest)
                update = handler_method[event_type](
                    self, message, last_message)

        return update

    def _handle_tzdf(self, correlation_id, bprequest, event_type, message_list, tzdf):
        try:
            new_request = self._handle_message_list(
                bprequest, event_type, message_list, tzdf)
        except:
            exc_info = sys.exc_info()
            bprequest._set_exc_info(exc_info)
            new_request = None
        else:
            self._update_handler(correlation_id, bprequest, new_request)

    def _handle_tzdf_error(self, correlation_id, bprequest, exc_info):
        exc_info = sys.exc_info()
        bprequest._set_exc_info(exc_info)
        self._update_handler(correlation_id, bprequest, None)

    def _update_handler(self, correlation_id, old_handler, new_handler):
        if new_handler:
            self._correlation_ids[correlation_id] = new_handler
        else:
            # Unsubscribe in case we are still subscribed. This happens when we are
            # cancelling the subscription even though it technically worked, which we
            # do when there are field exceptions.
            if old_handler.is_subscribed():
                subscription_list = blpapi.SubscriptionList()
                subscription_list.add(
                    '', correlationId=blpapi.CorrelationId(correlation_id))
                self._logger.debug(
                    'Unsubscribing from subscription with correlation ID %d...', correlation_id)
                self._session.unsubscribe(subscription_list)
            del self._correlation_ids[correlation_id]

    def __enter__(self):
        assert self.is_open()
        return self

    def __exit__(self, type, value, traceback):
        try:
            self.close()
        except SessionClosedError:
            pass

    def _send_auth_request(self, auth_request, service):
        request = service.createAuthorizationRequest()
        for k, v in six.iteritems(auth_request):
            if isinstance(k, six.string_types):
                k = str(k)
            if isinstance(v, six.string_types):
                v = str(v)
            request.set(k, v)
        correlation_id = self._generate_correlation_id()
        identity = self._session.createIdentity()

        self._session.sendAuthorizationRequest(
            request, identity, correlationId=blpapi.CorrelationId(correlation_id))
        self._logger.debug('Sent authorization request with %s', auth_request)

        promise = Promise(self.event_loop)
        self._correlation_ids[correlation_id] = AuthorizationRequest(
            promise, identity)
        self._correlation_ids[correlation_id].stz = None
        self._correlation_ids[correlation_id].dtz = None

        return promise

    # Deprecated functionality:
    @shim_warning
    def chain(self, *args, **kwargs):
        import pyrefdata
        return pyrefdata.chain(self, *args, **kwargs)

    @shim_warning
    def fields(self, *args, **kwargs):
        import pyrefdata
        return pyrefdata.resolve_fields(self, *args, **kwargs)

    @shim_warning
    def universe(self, *args, **kwargs):
        import pyrefdata
        return pyrefdata.universe(self, *args, **kwargs)
