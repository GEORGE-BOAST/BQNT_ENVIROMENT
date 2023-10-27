# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import sys

from .error import (
    RequestError,
    SecurityError,
    FieldError,
    SubscriptionClosedError,
    AuthorizationError,
    RequestTimedOutError)
from .promise import Promise


class ServiceInteraction(object):

    """Represents an ongoing interaction with a Bloomberg service.

    An instance of this class typically has a correlation ID assigned, and
    handles all messages received from the server that belong to that
    correlation ID."""

    def handle_session_terminated(self, session, exc_info):
        """Called when the session is terminated while the interaction is still ongoing."""
        pass

    def handle_partial_response(self, session, message, last_message):
        """Called when a partial response to a request has been received."""
        pass

    def handle_response(self, session, message, last_message):
        """Called when the final response to a request has been received."""
        pass

    def handle_request_status(self, session, message, last_message):
        """Called when a request status message has been received."""
        pass

    def handle_subscription_status(self, session, message, last_message):
        """Called when the subscription status for a subscription changes."""
        pass

    def handle_subscription_data(self, session, message, last_message):
        """Called when subscription data has been received."""
        pass

    def _set_exc_info(self, exc_info):
        """Called when an error occurred and the interaction needs to stop."""
        pass


class Request(ServiceInteraction):

    """Represents a request to the Bloomberg service.

    This class handles responses to requests to the Bloomberg service
    according to the Request/Response."""

    def __init__(self, promise):
        super(ServiceInteraction, self).__init__()
        self._promise = promise

    # Request is only used for Request/Response messages, not for subscriptions
    def is_subscribed(self):
        return False

    def handle_session_terminated(self, session, exc_info):
        self._promise.set_exc_info(exc_info)

    def handle_partial_response(self, session, message, last_message):
        try:
            self.on_handle_message(message)
            return self
        except Exception:
            exc_info = sys.exc_info()
            self._promise.set_exc_info(exc_info)
            return None

    def handle_response(self, session, message, last_message):
        try:
            self.on_handle_message(message)
            if last_message:
                result = self.get_result()
        except Exception:
            exc_info = sys.exc_info()
            self._promise.set_exc_info(exc_info)
            return None
        else:
            if last_message and not self._promise.done():
                # TODO: If this is a promise, chain the promise instead.
                self._promise.set_result(result)
                return None
            return self

    def handle_request_status(self, session, message, last_message):
        if message.message_type() == 'RequestFailure':
            raise RequestTimedOutError(message['reason']['description'])

    def on_handle_message(self, message):
        """Process a message received from a Bloomberg service."""
        assert False, 'SingleRequest subclass must override on_handle_message!'

    def get_result(self):
        """Return the processed result of the request."""
        assert False, 'SingleRequest subclass must override get_result!'

    def _set_exc_info(self, exc_info):
        self._promise.set_exc_info(exc_info)


class AuthorizationRequest(Request):

    """Represents an authorization request to the Bloomberg authentication API.

    When successful, yields an authorized identity."""

    def __init__(self, promise, identity):
        super(AuthorizationRequest, self).__init__(promise)
        self._identity = identity
        self._success = False

    def on_handle_message(self, message):
        if message.message_type() == 'AuthorizationSuccess':
            self._success = True
            self._promise.set_result(self._identity)
        elif message.message_type() == 'AuthorizationFailure':
            reason = message['reason']
            raise AuthorizationError(
                'Authorization failed: code={code}, msg={msg}, '
                'category={cat}, subcategory={subcat}, source={src}'
                .format(code=reason['code'],
                        msg=reason['message'],
                        cat=reason['category'],
                        subcat=reason['subcategory'],
                        src=reason['source']))

    def get_result(self):
        if not self._success:
            raise RequestError('Server did not send authorization result')
        return self._identity


class GenericRequest(Request):

    """Represents a generic Bloomberg request."""

    def __init__(self, promise, format):
        super(GenericRequest, self).__init__(promise)
        self._format = format

    def on_handle_message(self, message):
        self._format.update(message)

    def get_result(self):
        return self._format.finalize()


def _ignore_error(_exc_info):
    pass


class Subscription(ServiceInteraction):

    """Subscription to a specific topic.

    An instance of this class represents a Subscription to one specific
    topic with a Bloomberg service. It should not be instantiated
    directly, but with the :meth:`Session.subscribe()` or the more generic
    :meth:`Session.send_subscribe()` methods.

    The public interface of this class exposes three promises:
    :meth:`on_subscribe()`, :meth:`on_unsubscribe()` and :meth:`on_update()`.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~Subscription.current
        ~Subscription.on_subscribe
        ~Subscription.on_unsubscribe
        ~Subscription.on_update
    """

    class NoData(BaseException):
        pass

    def __init__(self, correlation_id, event_loop):
        super(Subscription, self).__init__()
        # correlation ID is public
        self.correlation_id = correlation_id
        self._event_loop = event_loop

        self._subscribe_promise = Promise(event_loop)
        self._unsubscribe_promise = Promise(event_loop)
        self._update_promise = Promise(event_loop, multi=True)
        self._subscribed = False

        self._current_value = None

        # Ignore all errors set on these promises. The reason for this is that these
        # promises are more like signals in a signal/slot system: when someone is
        # interested in these events, they can handle them, but there is not a clear
        # responsibility to who should handle the errors of these promises.
        # As soon as someone calls on_subscribe(), on_update() or on_unsubscribe(),
        # we give them a new promise without an error handler set, so that as soon
        # as one of these are called, unhandled exceptions will be reported.
        self._subscribe_promise.then(error_handler=_ignore_error)
        self._unsubscribe_promise.then(error_handler=_ignore_error)
        self._update_promise.then(error_handler=_ignore_error)

        # Public promises that we hand out in the on_() functions. These will report
        # unhandled exceptions.
        # TODO: Instead, in the on_() functions, we could simply remove the handler
        # installed above.
        self._public_subscribe_promise = None
        self._public_unsubscribe_promise = None
        self._public_update_promise = None

    def is_subscribed(self):
        return self._subscribed

    def set_format_and_topic(self, format, topic):
        self.format = format
        self.topic = topic

    def on_subscribe(self):
        """Return a :class:`Promise` which resolves when the subscription is established.

        When :meth:`Session.subscribe()` or :meth:`Session.send_subscribe()`
        return, the subscription is not yet confirmed by the service, and it
        might still fail for example if the topic to subscribe to does not
        exist.

        This function returns a :class:`Promise` which produces a value as
        soon as the subscription was confirmed by the service. It is guaranteed
        that no tick updates (see :meth:`on_update()`) are generated before
        this promise finishes. The result of the promise is the subscription
        object itself.

        If the subscription fails and cannot be established, this promise as
        well as :meth:`on_unsubscribe()` and :meth:`on_update()` will fail.
        """
        if not self._public_subscribe_promise:
            self._public_subscribe_promise = self._subscribe_promise.then()
        return self._public_subscribe_promise

    def on_unsubscribe(self):
        """Return a :class:`Promise` which resolves when the subscription is cancelled.

        This function returns a :class:`Promise` which produces a value when
        the subscription is cancelled. This can happen when the subscription cannot
        be established, when :meth:`Session.unsubscribe()` is called, or when the
        session is terminated.

        If the topic is unsubscribed normally, the promise will result in the
        subscription object itself, otherwise it will fail with an exception.
        """
        if not self._public_unsubscribe_promise:
            self._public_unsubscribe_promise = self._unsubscribe_promise.then()
        return self._public_unsubscribe_promise

    def on_update(self):
        """Return a :class:`Promise` which provides a value on each tick.

        This function returns a special promise which can deliver values
        repeatedly. It delivers a value every time the a tick is received
        from the service. When the topic is unsubscribed from, the promise
        will produce an error with the reason for the unsubscription.
        """
        if not self._public_update_promise:
            self._public_update_promise = self._update_promise.then()
        return self._public_update_promise

    def current(self, wait=True):
        """Return the values from the last subscription update.

        Optionally, the function blocks if there has not been any subscription
        update yet. Otherwise, ``None`` is returned in that case.

        Parameters
        ----------
            wait : bool
                If no subscription update has been received yet, ``True`` will
                cause the function to block until the first subscription update
                is received, while ``False`` will cause the function to return
                ``None``.
        """
        if self._current_value is None and wait:
            return self._update_promise.result()
        else:
            return self._current_value

    def handle_session_terminated(self, session, exc_info):
        if not self._subscribe_promise.done():
            self._subscribe_promise.set_exc_info(exc_info)
        self._update_promise.set_exc_info(exc_info)
        self._unsubscribe_promise.set_exc_info(exc_info)

    def handle_subscription_status(self, session, message, last_message):
        try:
            if message.message_type() == 'SubscriptionStarted':
                # Set this to true even if we had exceptions, because
                # technically we are subscribed and receive updates.
                self._subscribed = True
                for exception in message['exceptions']:
                    # If we have an exception, then throw an exception and cancel the
                    # entire subscription. The session object will unsubscribe us in
                    # this case.
                    field_id = exception['fieldId']
                    reason = exception['reason']['description']
                    raise FieldError('Failed to subscribe to topic "{}", field "{}": {}'.format(
                        self.topic, field_id, reason))
                # sub = Subscription(self.correlation_id, self._event_loop, self.format)
                self._subscribe_promise.set_result(self)
                session._broadcast('SubscriptionStarted', self)
                return self
            elif message.message_type() == 'SubscriptionFailure':
                if 'description' in message['reason']:
                    reason = message['reason']['description']
                else:
                    reason = 'Code {}'.format(message['reason']['errorCode'])
                raise SecurityError(
                    'Failed to subscribe to topic "{}": {}'.format(self.topic, reason))
            elif message.message_type() == 'SubscriptionTerminated':
                self._subscribed = False
                reason = message['reason']['description']
                session._broadcast("SubscriptionTerminated", self)
                raise SubscriptionClosedError(
                    'The subscription for topic "{}" has been terminated: {}'.format(self.topic, reason))
            return self
        except Exception as ex:
            exc_info = sys.exc_info()
            assert not self._unsubscribe_promise.done()

            if not self._subscribe_promise.done():
                self._subscribe_promise.set_exc_info(exc_info)
            self._update_promise.set_exc_info(exc_info)

            if isinstance(ex, SubscriptionClosedError):
                self._unsubscribe_promise.set_result(self)
            else:
                self._unsubscribe_promise.set_exc_info(exc_info)

            return None

    def handle_subscription_data(self, session, message, last_message):
        try:
            self.format.update(message)
            self._current_value = self.format.finalize()
        except Exception:
            exc_info = sys.exc_info()
            self._update_promise.set_exc_info(exc_info)
        except Subscription.NoData:
            pass
        else:
            self._update_promise.set_result(self._current_value)
        return self

    def _set_exc_info(self, exc_info):
        self._subscribe_promise.set_exc_info(exc_info)
        self._update_promise.set_exc_info(exc_info)
        self._unsubscribe_promise.set_exc_info(exc_info)
