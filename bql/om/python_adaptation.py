# Copyright 2022 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import keyword


def add_trailing_underscore_if_keyword(name):
    """Add a trailing underscore to `name` if 'name' is a Python keyword.

    Parameters
    ----------
    name: Text
        Metadata entity name.

    Returns
    -------
    name: Text
        Metadata entity name.
    """
    if keyword.iskeyword(name.lower()):
        name = f'{name}_'

    return name


def strip_trailing_underscore_if_keyword(name):
    """Removes a trailing underscore from 'name' if 'name' would be a Python
    keyword without it.

    Parameters
    ----------
    name: Text
        Metadata entity name.

    Returns
    -------
    name: Text
        Metadata entity name.
    """
    if name.endswith('_') and keyword.iskeyword(name[:-1].lower()):
        name = name[:-1]

    return name


def add_leading_underscore_if_numeric(name):
    """Add a leading underscore to 'name' if it begins with a numeric character.

    Parameters
    ----------
    name: Text
        Metadata entity name.

    Returns
    -------
    name: Text
        Metadata entity name.
    """
    if name[:1].isdigit():
        name = f'_{name}'

    return name


def strip_leading_underscore_if_numeric(name):
    """Removes a leading underscore from 'name' if 'name' begins with a numeric
    character without it.

    Parameters
    ----------
    name: Text
        Metadata entity name.

    Returns
    -------
    name: Text
        Metadata entity name.
    """
    if name.startswith('_') and name[1:][:1].isdigit():
        name = name[1:]

    return name


def add_formatting(name):
    """Applies the formatting needed to 'name' so that it can be used as
    a valid attribute in the pybql object model.

    Parameters
    ----------
    name: Text
        Metadata entity name.

    Returns
    -------
    name: Text
        Metadata entity name.
    """
    return add_leading_underscore_if_numeric(
        add_trailing_underscore_if_keyword(name))


def strip_formatting(name):
    """Removes the formatting applied to 'name' so that it can be used in a
    BQL query string.

    Parameters
    ----------
    name: Text
        Metadata entity name.

    Returns
    -------
    name: Text
        Metadata entity name.
    """
    return strip_leading_underscore_if_numeric(
        strip_trailing_underscore_if_keyword(name))
