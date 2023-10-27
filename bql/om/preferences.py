#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from ._validated_preferences import _ValidatedPreferences
import collections

class Preferences(object):
    __hash__ = None

    def __init__(self, **kwargs):
        # Standardize on lowercase names like we do for BqlItem parameters.
        self.__preferences = {k.lower(): v for (k, v) in kwargs.items()}

    def __repr__(self):
        return str(self.__preferences)

    def __getattr__(self, name):
        try:
            return self.__preferences[name.lower()]
        except KeyError:
            raise AttributeError(f"'{name}' is not set")

    def __len__(self):
        return len(self.__preferences)

    def __iter__(self):
        return iter(self.__preferences)

    def __hash__(self):
        return hash(self.__preferences)
    
    def __eq__(self, other):
        return (isinstance(other, Preferences) and
                self.__preferences == other.__preferences)

    def __ne__(self, other):
        return not (self == other)
    
    @staticmethod
    def create(preferences):
        if preferences is None:
            return Preferences()
        elif isinstance(preferences, collections.abc.Mapping):
            return Preferences(**preferences)
        else:
            # Assume that it's already a Preferences object.
            return preferences

    def to_dict(self):
        return dict(self.__preferences)

    def validate(self, metadata_reader):
        return _ValidatedPreferences(metadata_reader, **self.__preferences)
