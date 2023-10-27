#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from ._parameter_binding import _ParameterBinding
from bqlmetadata import ParameterGroup


class _ValidatedPreferences(object):
    __hash__ = None

    def __init__(self, metadata_reader, **kwargs):
        # Bind the preference values as if they were arguments to the one
        # parameter group to applyPreferences().
        metadata = metadata_reader._get_apply_preferences()
        assert len(metadata.parameter_groups) == 1
        pgroup = metadata.parameter_groups[0]
        # applyPreferences() needs an ITEM argument.  Remove that from the
        # ParameterGroup we're using here so we don't need to worry about it.
        params = [pgroup[pname] for pname in pgroup if pname != "param1"]
        id_ = 1
        pgroup = ParameterGroup(id_, pgroup.name, *params)
        self.__param_bindings = _ParameterBinding(pgroup,
                                                  positional_params=[],
                                                  named_params=kwargs)

    def __repr__(self):
        return str(self.to_dict())

    def __getattr__(self, name):
        # preserve normal behavior for dunder methods
        if name.startswith('__'):
            raise AttributeError(name)
        # Because we're using _ParameterBinding we get default values for free.
        return self.__param_bindings.get_value(name.lower())

    def __len__(self):
        return len(self.__param_bindings)

    def __iter__(self):
        return iter(self.to_dict())

    def __eq__(self, other):
        return (isinstance(other, _ValidatedPreferences) and
                self.__param_bindings == other.__param_bindings)

    def __ne__(self, other):
        return not (self == other)

    def to_dict(self):
        # Note that this returns a dict only of preferences that were specified
        # (unspecified preferences and their default values are not returned).
        return self.__param_bindings.named_parameters

    def format_for_query_string(self):
        return self.__param_bindings.format_for_query_string()

    def validate(self, metadata_reader):
        # Although the preferences have already been validated, they may not
        # have been validated against this metadata_reader, so do it again.
        return _ValidatedPreferences(metadata_reader, **self.to_dict())
