# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import collections
import datetime
import dateutil
import dateutil.tz

import blpapi
import six


def wrap(elem, stz, dtz):
    """Wrap an inidivdual BLPAPI element in a pythonic data structure."""
    if not isinstance(elem, blpapi.Element):
        if isinstance(elem, datetime.datetime):
            if stz is not None and elem.tzinfo is None:
                elem = elem.replace(tzinfo=stz)
            if elem.tzinfo is not None and dtz is not None:
                elem = elem.astimezone(dtz)
            return elem
        if isinstance(elem, datetime.time):
            today = datetime.date.today()
            dt = datetime.datetime(year=today.year, month=today.month, day=today.day,
                                   hour=elem.hour, minute=elem.minute, second=elem.second,
                                   microsecond=elem.microsecond, tzinfo=elem.tzinfo)

            if stz is not None and dt.tzinfo is None:
                dt = dt.replace(tzinfo=stz)
            if dt.tzinfo is not None and dtz is not None:
                dt = dt.astimezone(dtz)
            return dt
        elif isinstance(elem, blpapi.Name):
            return str(elem)
        else:
            return elem
    elif elem.isArray():
        return ListElement(elem, stz, dtz)
    elif elem.isComplexType():
        return DictElement(elem, stz, dtz)
    elif elem.isNull():
        return None  # or NaN?
    else:
        value = elem.getValue()
        return wrap(value, stz, dtz)


class DictElement(collections.abc.Mapping):

    """Pythonic wrapper for a dict-like BLPAPI element.

    This object allows the common operations such as `len()`, dictionary-like
    lookup of sub-elements and iteration over keys and items.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~DictElement.__init__
        ~DictElement.items

    .. automethod:: DictElement.__init__
    """

    def __init__(self, blpapi_element, stz=None, dtz=None):
        """Constructs a :class:`DictElement` from a raw BLPAPI :class:`blpapi.Element` instance.

        Parameters
        ----------
        blpapi_element : :class:`blpapi.Element`
            A raw :class:`blpapi.Element` of type ``SEQUENCE`` or ``CHOICE``.
        stz : :class:`datetime.tzinfo`
            The timezone in which to interpret all non-timezone-aware
            time and datetime sub-elements.
        dtz : :class:`datetime.tzinfo`
            The timezone into which to convert all time and datetime
            sub-elements.
        """
        assert blpapi_element.datatype(
        ) == blpapi.DataType.SEQUENCE or blpapi_element.datatype() == blpapi.DataType.CHOICE
        self._blpapi_element = blpapi_element
        self.stz = stz
        self.dtz = dtz
        self.wrapper = wrap

    def items(self):
        """Return a generator to iterate over key-value pairs."""

        # This is inherited from the ABC in principle, but we are able to
        # provide a more efficient implementation.
        if self._blpapi_element.datatype() == blpapi.DataType.SEQUENCE:
            for elem in self._blpapi_element.elements():
                yield str(elem.name()), self.wrapper(elem, self.stz, self.dtz)
        else:
            choice = self._blpapi_element.getChoice()
            yield str(choice.name()), self.wrapper(choice, self.stz, self.dtz)

    def __len__(self):
        return self._blpapi_element.numElements()

    def __iter__(self):
        if self._blpapi_element.datatype() == blpapi.DataType.SEQUENCE:
            for elem in self._blpapi_element.elements():
                yield str(elem.name())
        else:
            choice = self._blpapi_element.getChoice()
            yield str(choice.name())

    def __getitem__(self, key):
        # If the element does not exist, re-raise as KeyError
        try:
            elem = self._blpapi_element.getElement(key)
        except blpapi.NotFoundException as ex:
            raise KeyError(key)

        return self.wrapper(elem, self.stz, self.dtz)

    def __contains__(self, key):
        # This is inherited from the ABC in principle, but we are able to
        # provide a more efficient implementation.
        return self._blpapi_element.hasElement(key)

    def __repr__(self):
        return repr(dict(six.iteritems(self)))


class ListElement(collections.abc.Iterable, collections.abc.Sized):

    """Pythonic wrapper for a list-like BLPAPI element.

    This object allows the common operations such as `len()`, iteration
    over the list, and lookup by index.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~ListElement.__init__

    .. automethod:: ListElement.__init__
    """

    def __init__(self, blpapi_element, stz=None, dtz=None):
        """Constructs a :class:`ListElement` from a raw BLPAPI :class:`blpapi.Element` instance.

        Parameters
        ----------
        blpapi_element : :class:`blpapi.Element`
            A raw :class:`blpapi.Element` which is an array.
        stz : :class:`datetime.tzinfo`
            The timezone in which to interpret all non-timezone-aware
            time and datetime sub-elements.
        dtz : :class:`datetime.tzinfo`
            The timezone into which to convert all time and datetime
            sub-elements.
        """
        assert blpapi_element.isArray()
        self._blpapi_element = blpapi_element
        self.stz = stz
        self.dtz = dtz
        self.wrapper = wrap

    def __len__(self):
        return self._blpapi_element.numValues()

    def __iter__(self):
        for elem in self._blpapi_element.values():
            yield self.wrapper(elem, self.stz, self.dtz)

    def __getitem__(self, index):
        return self.wrapper(self._blpapi_element.getValue(index), self.stz, self.dtz)

    def __repr__(self):
        return repr(list(self))


class Message(object):

    """Pythonic wrapper for a BLPAPI message.

    This class represents an individual message received from a Bloomberg
    service. Use the :meth:`~Message.element` method to access the
    contents of the message.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~Message.__init__
        ~Message.correlation_ids
        ~Message.message_type
        ~Message.element

    .. automethod:: Message.__init__
    """

    def __init__(self, blpapi_message, stz=None, dtz=None):
        self._blpapi_message = blpapi_message
        self.stz = stz
        self.dtz = dtz
        self.wrapper = wrap

    def correlation_ids(self):
        """Return a list of all correlation IDs associated with this message."""
        return [id.value() for id in self._blpapi_message.correlationIds()]

    def message_type(self):
        """Return the type of the message as a string."""
        return str(self._blpapi_message.messageType())

    def element(self):
        return self.wrapper(self._blpapi_message.asElement(), self.stz, self.dtz)


class MessageElement(DictElement, Message):

    """Wrapper for a BLPAPI message whose top-level element is a DictElement.

    This class inherits from :class:`DictElement`, and therefore can be used
    just like a dictionary. It also inherits from :class:`Message`, and can
    therefore be queried for its message type and correlation IDs.

    It represents an inidividual message received from
    a Bloomberg service.

    .. rubric:: Methods
    .. autosummary::
        :nosignatures:

        ~MessageElement.__init__

    .. automethod:: MessageElement.__init__
    """

    def __init__(self, blpapi_message, stz=None, dtz=None):
        """Constructs a :class:`MessageElement` from a raw BLPAPI :class:`blpapi.Message` instance.

        Parameters
        ----------
        blpapi_message : :class:`blpapi.Message`
            A raw :class:`blpapi.Message` instance.
        stz : :class:`datetime.tzinfo`
            The timezone in which to interpret all non-timezone-aware
            datetimes and times in the message.
        dtz : :class:`datetime.tzinfo`
            The timezone into which to convert all time and datetime
            sub-elements.
        """
        element = blpapi_message.asElement()
        DictElement.__init__(self, element, stz, dtz)
        Message.__init__(self,  blpapi_message, stz, dtz)


def fill_element(element, value):
    """Fill a BLPAPI element from native python types.

    This function maps dict-like objects (mappings), list-like
    objects (iterables) and scalar values to the BLPAPI element
    structure and fills `element` with the structure given
    in `value`.
    """

    def prepare_value(value):
        if isinstance(value, datetime.datetime):
            # Convert datetime objects to UTC for all API requests
            return value.astimezone(dateutil.tz.tzutc())
        else:
            return value

    if isinstance(value, collections.abc.Mapping):
        for name, val in six.iteritems(value):
            fill_element(element.getElement(name), val)
    elif isinstance(value, collections.abc.Iterable) and not isinstance(value, six.string_types):
        for val in value:
            # Arrays of arrays are not allowed
            if isinstance(val, collections.abc.Mapping):
                fill_element(element.appendElement(), val)
            else:
                element.appendValue(prepare_value(val))
    else:
        if element.datatype() == blpapi.DataType.CHOICE:
            element.setChoice(prepare_value(value))
        else:
            element.setValue(prepare_value(value))


class Event:

    def __init__(self, blpapi_event):
        self._blpapi_event = blpapi_event

    def __iter__(self):
        for msg in self._blpapi_event:
            element = msg.asElement()
            if element.datatype() == blpapi.DataType.SEQUENCE or element.datatype() == blpapi.DataType.CHOICE:
                yield MessageElement(msg)
            else:
                yield Message(msg)
