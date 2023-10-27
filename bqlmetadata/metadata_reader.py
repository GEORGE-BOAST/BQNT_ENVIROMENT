#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from abc import ABCMeta, abstractmethod

import six


def _check_what_arg(what):
    valid_whats = ('function', 'data-item', 'universe-handler')
    if what == 'all':
        return valid_whats
    else:
        if isinstance(what, six.string_types):
            what = (what,)

        for item in what:
            if item not in valid_whats:
                valid_whats_str = '", "'.join(valid_whats)
                raise ValueError(
                    f'The `what` can only contain any of "{valid_whats_str}"')

        return what


@six.add_metaclass(ABCMeta)
class MetadataReader():
    """Interface for consuming BQL metadata.

    This is an abstract class that defines an interface to consume BQL
    metadata.  Specific implementations of this abstract base class are
    :class:`SqliteCacheMetadataReader`, and :class:`MemoryMetadataReader`.

    .. autosummary::
        :nosignatures:

        ~MetadataReader.enumerate_names
        ~MetadataReader.get_by_name
        ~MetadataReader.get_by_operator
        ~MetadataReader.get_data_items
        ~MetadataReader.get_functions
        ~MetadataReader.get_universe_handlers
        ~MetadataReader._enumerate_names
        ~MetadataReader._get_by_name
        ~MetadataReader._get_by_operator

    .. automethod:: MetadataReader.__init__
    .. automethod:: MetadataReader._get_by_name
    .. automethod:: MetadataReader._get_by_operator
    .. automethod:: MetadataReader._enumerate_names

    """
    @abstractmethod
    def _get_by_name(self, what, name):
        """Overriden by implementations to implement :meth:`get_by_name`.

        This function should not be called directly. It is an abstract method
        that should be implemented by custom :class:`MetadataReader`
        implementations. The `what` argument is a tuple of ``'function'``,
        ``'data-item'``, ``'universe-handler'``, or a combination of them.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_by_operator(self, symbol, location):
        """Overriden by implementations to implement :meth:`get_by_operator`.

        This function should not be called directly. It is an abstract method
        that should be implemented by custom :class:`MetadataReader`
        implementations. See :meth:`~MetadataReader.get_by_operator` for
        the meaning of the arguments.
        """
        raise NotImplementedError

    @abstractmethod
    def _enumerate_names(self, what):
        """Overriden by implementations to implement :meth:`enumerate_names`.

        This function should not be called directly. It is an abstract method
        that should be implemented by custom :class:`MetadataReader`
        implementations. The `what` argument is a tuple of ``'function'``,
        ``'data-item'``, ``'universe-handler'``, or a combination of them.
        """
        raise NotImplementedError

    @abstractmethod
    def get_bulk_metadata(self):
        raise NotImplementedError

    def search_items(self, query, what='all', max_items=10):
        """Returns a list of metadata items matching the `query`

        `query` argument is the substring to search for within the name/mnemonic and/or description of items
        `what` argument is a tuple of ``'function'``, ``'data-item'``, ``'universe-handler'``,
        or a combination of them. `what` defaults to all of them combined ``'all'``.
        `max_items` is the number of max items per category and defaults to 10.
        """
        return self._search_items(query, _check_what_arg(what), max_items)

    def _search_items(self, query, what, max_items):
        """Overriden by implementations.
        This function should not be called directly. It is an abstract method
        that should be implemented by custom :class:`MetadataReader`
        implementations. `query`, `what` and `max_items` can be supplied as arguments
        `query` argument is the substring to search for within the name/mnemonic and/or description of items
        `what` argument is a tuple of ``'function'``, ``'data-item'``, ``'universe-handler'``,
        or a combination of them ``'all'``.
        `max_items` is the number of max items per category.
        """
        raise NotImplementedError

    def get_by_name(self, what, name):
        """Return a list of metadata for all items with the given `name`.

        Items with either the short name or mnemonic identical to `name`
        will be returned, as well as items which have `name` as an alias.
        Returns a list of :class:`MetadataItem` instances. Returns an empty
        list if there are no such items with the given name.

        The returned list is in no particular order.

        The `what` parameter can be ``'function'``, ``'data-item'``,
        ``'universe-handler'``, ``'all'``, or a tuple consisting of several
        of them.
        """
        return self._get_by_name(_check_what_arg(what), name)

    def get_by_operator(self, symbol, location):
        """Return a :class:`MetadataItem` representing the given operator.

        The `symbol` parameter should be the BQL operator symbol, such as
        ``'+'``, ``'-'``, or ``'AND'``. The `location` parameter should be
        one of ``'infix'`` or ``'prefix'``.

        Returns ``None`` if there is no such operator with the given symbol
        or location.
        """
        assert location in ('infix', 'prefix')
        return self._get_by_operator(symbol, location)

    def enumerate_names(self, what):
        """Return an iterable that yields the names of all metadata items.

        The returned list contains all items, mnemonics and aliases for all
        functions, data items, or both. Multiple strings in the returned list
        may correspond to the same item, e.g. ``'pr005'`` and ``'px_last'``
        will both refer to the same item.

        The `what` parameter can be ``'function'``, ``'data-item'``,
        ``'universe-handler'``, ``'all'``, or a tuple consisting of
        several of them.
        """
        return self._enumerate_names(_check_what_arg(what))

    def get_functions(self, name):
        """Return all functions with a given name.

        This is a shortcut for ``get_by_name('function', name)``.
        """
        return self.get_by_name('function', name)

    def get_data_items(self, name):
        """Return all data items with a given name.

        This is a shortcut for ``get_by_name('data-item', name)``.
        """
        return self.get_by_name('data-item', name)

    def get_universe_handlers(self, name):
        """Return all universe handlers with a given name.

        This is a shortcut for ``get_by_name('universe-handler', name)``.
        """
        return self.get_by_name('universe-handler', name)

    def get_version(self):
        """Return `VersionInfo` for the metadata."""
        raise NotImplementedError()

    def _get_apply_preferences(self):
        # Because we use the applyPreferences() metadata internally to validate
        # preferences, provide a convenience function to make the metadata easy
        # to access.
        metadata = self.get_by_name("function", "applyPreferences")
        assert len(metadata) == 1
        return metadata[0]
