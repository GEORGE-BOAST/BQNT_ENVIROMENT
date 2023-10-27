import collections
from typing import Dict, Optional

from .session import Session

_instances = {}


def _hashable(obj: object) -> object:
    """Returns a hashable representation of the passed object.

    For object types that natively support hashing (have a __hash__() method),
    this method simply returns the same object.  For object types that do not
    natively support hashing, it wraps the original object's values in
    tuples or a frozenset of tuples.

    Parameters
    ----------
    obj:
        The object whose hashable representation we are looking for.

    Raises
    ------
    TypeError:
        On unsupported types.

    Returns
    -------
    items:
        A hashable representation of the passed object.
    """
    if isinstance(obj, collections.abc.Hashable):
        items = obj
    elif isinstance(obj, collections.abc.Mapping):
        items = frozenset((k, _hashable(v)) for k, v in obj.items())
    elif isinstance(obj, collections.abc.Iterable):
        items = frozenset(_hashable(item) for item in obj)
    else:
        raise TypeError(f'Unsupported type: {type(obj)}')

    return items


def get_session_singleton(
        settings: Optional[Dict[str, any]] = None) -> Session:
    """
    Returns a reference to the singleton instance of the blp api session for
    the passed bqapi settings dictionary.  This function is not threadsafe
    and should not be invoked concurrently from multiple threads.
    The optional settings dictionary contains the bqapi session settings
    (see settings.py for an example of this dict).

    Parameters
    ----------
    settings:
        The (optional) bqapi session settings dict.

    Returns
    -------
    session:
        A reference to the singleton instance of the blp api session for
        the passed bqapi settings dict.
    """

    global _instances

    # convert the passed settings dict into a cache key
    key = _hashable(settings)

    # see if we already have a blp api session created for this cache key
    if key not in _instances:
        # create a new blp api session, store it in the cache with key
        _instances[key] = Session(**(settings or {}))

    return _instances[key]
