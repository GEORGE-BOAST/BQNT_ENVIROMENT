# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

class ValidationError(RuntimeError):
    # Base class for all BQL object model errors.

    # All errors that can occur when constructing a BQL query with the object
    # model derive from this base class."""
    pass


class InvalidParameterNameError(ValidationError):
    # Raised when there is no such parameter with a given name.
    pass


class InvalidParameterValueError(ValidationError):
    # Raised when an invalid python value is provided for a parameter.

    # For example, if a string that does not represent a date is used for a
    # parameter of type DATE.
    pass


class UnexpectedParameterError(ValidationError):
    # Raised when a parameter is provided that was not expected.

    # For example, if a function that takes at most 2 parameters is given 3
    # positional arguments.
    pass


class MissingParameterError(ValidationError):
    # Raised when a mandatory parameter is not provided.
    pass


class InvalidParameterGroupError(ValidationError):
    # Raised when no parameter group can accomodate the given set of parameters.
    def __init__(self, name, pgroups, individidual_errors):
        self._pgroups = [pgroup.name for pgroup in pgroups]
        self._errors = list(individidual_errors)
        if not self._errors:
            msg = f'"{name}" does not expect any parameters'
        elif len(self._errors) == 1:
            msg = f'Invalid parameters for "{name}": {self._errors[0]}'
        else:
            description = '\n'.join([
                f'Parameter group "{pgroup}" does not work because: {error}'
                for pgroup, error in zip (self._pgroups, self._errors)
            ])
            msg = (f'No parameter group of "{name}" can accomodate the '
                   f'given set of parameters.\n\n{description}')
        super(InvalidParameterGroupError, self).__init__(msg)

    @property
    def parameter_groups(self):
        return self._pgroups

    @property
    def individual_errors(self):
        return self._errors
