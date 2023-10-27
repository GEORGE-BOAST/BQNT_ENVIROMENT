# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from abc import ABCMeta, abstractmethod
import datetime
import numpy as np

import six

from .relative_date import RelativeDate

# These functions are used for equality comparisons in the singleton data
# types.


def _singleton_eq(x, y):
    return type(x) is type(y)


def _singleton_ne(x, y):
    return not x == y


def _singleton_hash(x):
    return hash(type(x)) ^ hash(())


# Note that we do fairly strong type checking when validating literals.
# This is due to the fact that if there are two parameter groups with
# two different types, we want to be able to disambiguate which one the
# user wants, and generate the appropriate BQL query string for it.

# TODO: A maybe better solution would be to allow both, but then
# prioritize the one with more "direct" parameters when generating
# the BQL query string.


class LiteralTypeError(TypeError):
    """Raised if a python value cannot be converted to a BQL literal.

    For example, if the python string ``'abc'`` was attempted to be converted
    to an integer BQL literal.
    """


@six.add_metaclass(ABCMeta)
class Literal():
    """Represents a class of literals for a certain BQL type.

    This interface specifies how to represent literals in a BQL string in
    Python, i.e. which python type to use to store the literal value, and also
    how to render a literal value of this type to BQL request string.
    """
    @abstractmethod
    def validate(self, val):
        """Validate and canonicalizes a literal value.

        Validates that the given python value is of the correct type for this
        literal, or can be converted to it. It raises :class:`LiteralTypeError`
        if there is no such conversion.

        Returns the value converted to the canonical type for this data type.
        """
        raise NotImplementedError()

    @abstractmethod
    def to_bql(self, val):
        """Return a string representing in BQL syntax for this literal.

        The value must be of the canonical type for the data type
        represented by this class, as produced by :meth:`validate`.
        """
        raise NotImplementedError()

    def format_for_docstring(self, val):
        """Return a string representation suitable for docstring help."""
        # Derived classes can override this if necessary.
        return self.to_bql(val)


class IntLiteral(Literal):
    def validate(self, val):
        # Don't allow float -> int conversions since they would lose precision.
        if isinstance(val, float):
            raise LiteralTypeError(
                f'Converting the floating point value {val} to INT would lose '
                'precision')
        try:
            return int(val)
        except (TypeError, ValueError):
            try:
                return NA.validate(val)
            except LiteralTypeError:
                # val isn't a number and isn't NA.
                raise LiteralTypeError(f'Not an integer: "{val}"')

    def to_bql(self, val):
        if val is NA:
            return NA.to_bql(val)
        else:
            # Integer literals are represented directly.
            return f'{val}'

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


class StringLiteral(Literal):
    def validate(self, val):
        if not isinstance(val, six.string_types):
            try:
                return NA.validate(val)
            except LiteralTypeError:
                raise LiteralTypeError(f'Not a string: "{val}"')

        if '"' in val:
            raise LiteralTypeError(
                f"Unsupported double quote character in string literal {val}")

        return str(val)

    def to_bql(self, val):
        if val is NA:
            return NA.to_bql(val)
        else:
            return f"'{val}'"

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


class DoubleLiteral(Literal):
    def validate(self, val):
        try:
            return float(val)
        except (TypeError, ValueError):
            try:
                return NA.validate(val)
            except LiteralTypeError:
                # val isn't a number and isn't NA.
                raise LiteralTypeError(f'Not a floating point number: "{val}"')

    def to_bql(self, val):
        if val is NA:
            return NA.to_bql(val)
        else:
            # Note this makes sure to always use the period (.) character as
            # decimal separator, independent of the locale.
            #
            # Since BQL treats the positive sign before the exponent as an
            # addition operator, we need to remove it from the scientific
            # notation representation.
            return str(val).replace('e+', 'e')

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


class BoolLiteral(Literal):
    def validate(self, val):
        if isinstance(val, six.string_types):
            if val.lower() in ('true', 'false'):
                return bool(val.lower())
            elif val == "1" or val == "0":
                return bool(int(val))
            else:
                try:
                    return NA.validate(val)
                except LiteralTypeError:
                    raise LiteralTypeError(f'Not a boolean: "{val}"')
        elif isinstance(val, (bool, int)):
            return bool(val)
        else:
            try:
                return NA.validate(val)
            except LiteralTypeError:
                # val isn't a boolean and isn't NA
                raise LiteralTypeError(f'Not a boolean: "{val}"')

    def to_bql(self, val):
        if val is NA:
            return NA.to_bql(val)
        elif val:
            return 'true'
        else:
            return 'false'

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


class DateLiteral(Literal):
    """Literal representing a BQL DATE type.

    A BQL DATE is actually a datetime, so the canonical python representation
    is a naive :class:`datetime.datetime` in UTC.

    BQL DATEs can also be specified in a relative format, such as "-3Y". For
    relative dates, the canonical type is :class:`bql.RelativeDate`.
    """

    # TODO(aburgm): Return aware datetimes? Python 3 has datetime.timezone.utc
    # representing UTC timezone
    def validate(self, val):
        if isinstance(val, six.string_types):
            validators = [
                lambda v: datetime.datetime.strptime(v, "%Y-%m-%d"),
                RelativeDate.from_string,
                # If we can't interpret a date string as either a Python
                # datetime object or a RelativeDate, then liberally treat it as
                # a valid BQL date string (e.g., 2016Q1).  We'll let the BQL
                # backend raise an error if it's not a valid date string.
                #
                # Note that we're keeping RelativeDate for backwards
                # compatibility: in reality any RelativeDate like -5D could
                # simply pass through to the str validator.
                str
            ]
            for validator in validators:
                try:
                    return validator(val)
                except ValueError:
                    # Try the next validator.
                    pass
            # We should never get to this point: the str validator should
            # catch any six.string_types.
            assert False, f'"{val}" not accepted as date string'
        elif isinstance(val, RelativeDate):
            return val
        elif isinstance(val, datetime.datetime):
            # If val is aware, then convert to UTC and make it naive
            if val.utcoffset() is None:
                return val.replace(tzinfo=None)
            else:
                return val.replace(tzinfo=None) - val.utcoffset()
        elif isinstance(val, datetime.date):
            return datetime.datetime.combine(val, datetime.time())
        # TODO: Support np.datetime64?
        else:
            try:
                return NA.validate(val)
            except LiteralTypeError:
                raise LiteralTypeError(f'Not a date: "{val}"')

    def to_bql(self, val):
        # We assume that val has already been validated.
        if val is NA:
            return NA.to_bql(val)
        elif isinstance(val, RelativeDate):
            return str(val)
        elif isinstance(val, datetime.datetime):
            return val.strftime("%Y-%m-%d")
        else:
            # Date string (e.g., 2016Q1).
            return val

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


class EnumLiteral(Literal):
    """Literal representing an ENUM data type.

    Valid literals are python strings that correspond to any of the
    enumerants specified in the constructor.
    """

    def __init__(self, enum_name, enumerants):
        # Enum names are prefixed with a namespace like "CR.FILL", but we
        # only care about the tail part of the name when generating query
        # strings.
        self._enum_name = enum_name.split(".")[-1]
        self._enumerants = sorted(enumerants)
        # Uppercase enumerants used for case-insensitive validation.
        self._upper_enumerants = set(x.upper() for x in enumerants)

    @property
    def enumerants(self):
        return self._enumerants

    def validate(self, val):
        if not isinstance(val, six.string_types):
            raise LiteralTypeError(f'Not a string: "{val}"')

        # Treat the value as a string.  Even though some enumerated values
        # are case sensitive (e.g., GBP vs GBp), we validate in a case
        # insensitive way.  We leave it up to BQL to sort out how to deal
        # with values such as GbP.
        val = str(val)
        if val.upper() not in self._upper_enumerants:
            enumerants = '", "'.join(self._enumerants)
            raise LiteralTypeError(
                f'"{val}" is not a valid enumerant. '
                f'Valid enumerants are "{enumerants}"')

        # ENG2BQNTFL-619: Preserve original case of enumerated value.
        return val

    def to_bql(self, val):
        return f"'{val}'"

    def format_for_docstring(self, val):
        # Do not prefix the enumerant
        return val

    def __eq__(self, other):
        return (type(self) is type(other) and
                self._enum_name == other._enum_name and
                self._enumerants == other._enumerants)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(type(self)) ^ hash((self._enum_name, self._enumerants))


class NaLiteral(Literal):
    """Literal representing an NA value."""
    @staticmethod
    def is_na(val):
        return val is NA or val == NaLiteral.string()

    @staticmethod
    def string():
        return "NA"

    def validate(self, val):
        if NaLiteral.is_na(val):
            return NA
        raise LiteralTypeError(f'Not a NA value: "{val}"')

    def to_bql(self, val):
        # We assume the value has always been validated.
        assert val is NA
        return self.string()

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


# Singleton object that you use in the OM to reference an NA value.  Note
# that is is the only thing that we treat as a BQL NA value when generating
# the query string.
NA = NaLiteral()


def is_nan(val):
    try:
        return np.isnan(val)
    except TypeError:
        # np.isnan() throws an exception for value types that it doesn't
        # recognize (like strings).  These are not NAs.
        return False


class AnyScalarLiteral(Literal):
    """Literal for BQL's ANY_SCALAR type.

    The BQL type is deduced from the type of the python object. The
    "canonical" python type is then a AnyScalarLiteral.Value object that
    binds the value with the actual BQL type deduced.
    """

    class Value(object):
        def __init__(self, dtype, val):
            self.__dtype = dtype
            self.__value = val

        @property
        def dtype(self):
            return self.__dtype

        @property
        def value(self):
            return self.__value

        def to_bql(self):
            return self.__dtype.to_bql(self.__value)

        def __eq__(self, other):
            eq = (type(self) is type(other) and
                  self.dtype == other.dtype)
            if eq:
                eq = (
                    (self.value == other.value) or
                    # Treat np.nan as a special case, since np.nan != np.nan.
                    (is_nan(self.value) and is_nan(other.value))
                )
            return eq

        def __ne__(self, other):
            return not self == other

        def __hash__(self):
            return hash(type(self)) ^ hash((self.dtype, self.value))

    def validate(self, val):
        # Infer the BQL type from the python type.
        if isinstance(val, float):
            dtype = DoubleLiteral()
        elif isinstance(val, int):
            dtype = IntLiteral()
        elif isinstance(val, bool):
            dtype = BoolLiteral()
        elif isinstance(val, six.string_types):
            dtype = StringLiteral()
        elif isinstance(val, datetime.date):  # includes datetime.datetime
            dtype = DateLiteral()
        elif isinstance(val, AnyScalarLiteral.Value):
            dtype = val.dtype
            val = val.value
        elif NaLiteral.is_na(val):
            dtype = NaLiteral()
        else:
            raise LiteralTypeError(
                f'Value "{val}" (type "{str(type(val))}") is not a scalar')

        return AnyScalarLiteral.Value(dtype, dtype.validate(val))

    def to_bql(self, val):
        assert isinstance(val, AnyScalarLiteral.Value)
        return val.to_bql()

    __eq__ = _singleton_eq
    __ne__ = _singleton_ne
    __hash__ = _singleton_hash


class DataTypes:
    # RDSIBQUN-1920: Validate UNKNOWN like we do ANY_SCALAR.
    UNKNOWN = ('UNKNOWN', 0, AnyScalarLiteral)
    INT = ('INT', 1, IntLiteral)
    ENUM = ('ENUM', 2, EnumLiteral)
    BOOLEAN = ('BOOLEAN', 3, BoolLiteral)
    DOUBLE = ('DOUBLE', 4, DoubleLiteral)
    DATE = ('DATE', 5, DateLiteral)
    STRING = ('STRING', 6, StringLiteral)
    ITEM = ('ITEM', 7, None)
    # UNIVERSE is a special type of ITEM that is a universe handler.
    UNIVERSE = ('UNIVERSE', 8, None)
    ANY_SCALAR = ('ANY_SCALAR', 9, AnyScalarLiteral)
    DERIVED = ('DERIVED', 10, None)
    DATETIME = ('DATETIME', 11, DateLiteral)


class DataTypeMap:
    def __init__(self, types):
        self._types_by_name = {
            name: (name, id_, obj) for name, id_, obj in types
        }
        self._literals_by_id = {
            id_: (name, id_, obj) for name, id_, obj in types
        }

    def type_from_name(self, name):
        """Given a BQL type name, return the datatype tuple."""
        return self._types_by_name[name.upper()]

    def type_from_number(self, num):
        """Given an internal type ID number, return the datatype tuple."""
        return self._literals_by_id[num]


ParameterTypes = DataTypeMap([DataTypes.UNKNOWN,  DataTypes.INT,
                              DataTypes.ENUM,     DataTypes.BOOLEAN,
                              DataTypes.DOUBLE,   DataTypes.DATE,
                              DataTypes.STRING,   DataTypes.ITEM,
                              DataTypes.UNIVERSE, DataTypes.ANY_SCALAR,
                              DataTypes.DATETIME])

ColumnTypes = DataTypeMap([DataTypes.UNKNOWN, DataTypes.INT,
                           DataTypes.BOOLEAN, DataTypes.DOUBLE,
                           DataTypes.DATE,    DataTypes.STRING,
                           DataTypes.ENUM,    DataTypes.DATETIME])

ReturnTypes = DataTypeMap([DataTypes.UNKNOWN, DataTypes.DERIVED])


def make_parameter_literal_from_number(num, enum_name, enumerants):
    name, num, literal = ParameterTypes.type_from_number(num)

    literal_obj = None
    if literal == EnumLiteral:
        literal_obj = literal(enum_name, enumerants)
    elif literal is not None:
        literal_obj = literal()

    return (name, num, literal), literal_obj


def check_type_conversion(from_type, to_type):
    # Represents allowed type conversions of BQL types amongst another.
    # Note this is not about which Python types we allow to convert to
    # which BQL types -- instead, this is implictly defined in the various
    # valiadte methods in the literal classes above.

    # Note further, that, at the moment, this table will not be used for
    # anything but ITEM and DERIVED, because the return type of all functions
    # is DERIVED and all data items are of type ITEM. If/when BACC-1775 gets
    # implemented, we might be able to do more sophisticated type checking.
    TYPE_CONV_TABLE = {
        # From: To
        DataTypes.UNKNOWN:    [DataTypes.UNKNOWN, DataTypes.ANY_SCALAR],
        # int -> string?
        DataTypes.INT:        [DataTypes.INT, DataTypes.BOOLEAN,
                               DataTypes.DOUBLE, DataTypes.ANY_SCALAR,
                               DataTypes.UNKNOWN],
        DataTypes.ENUM:       [DataTypes.ENUM, DataTypes.STRING,
                               DataTypes.ANY_SCALAR, DataTypes.UNKNOWN],
        # bool -> string?
        DataTypes.BOOLEAN:    [DataTypes.INT, DataTypes.BOOLEAN,
                               DataTypes.DOUBLE, DataTypes.ANY_SCALAR,
                               DataTypes.UNKNOWN],
        DataTypes.DOUBLE:     [DataTypes.DOUBLE, DataTypes.ANY_SCALAR,
                               DataTypes.UNKNOWN],
        DataTypes.DATE:       [DataTypes.DATE, DataTypes.ANY_SCALAR,
                               DataTypes.UNKNOWN],
        # string -> date?
        DataTypes.STRING:     [DataTypes.DATE, DataTypes.STRING,
                               DataTypes.ANY_SCALAR, DataTypes.UNKNOWN],
        DataTypes.ITEM:       [DataTypes.ITEM],
        DataTypes.UNIVERSE:   [DataTypes.UNIVERSE],
        DataTypes.ANY_SCALAR: [DataTypes.ANY_SCALAR, DataTypes.UNKNOWN],
        # "DERIVED" is used as the return type for all functions, and it
        # indicates that the return value often depends on the types of the
        # parameters (e.g., in an if() function).  For validation purposes,
        # therefore, we say that a function can be bound to almost any
        # parameter type.  That's certainly true of literals because we can't
        # rule out that a function won't return one of the literal values, and
        # it's also true of ITEMs (i.e., any data item or function that takes
        # an ITEM parameter will accept a function for that parameter).  The
        # one exception is for universe handlers: we don't want a universe
        # parameter to be validated successfully against a function (see
        # ENG2BQNTFL-510).
        # TODO(kwhisnant): Include UNKNOWN too?
        DataTypes.DERIVED:    [DataTypes.INT, DataTypes.ENUM,
                               DataTypes.BOOLEAN, DataTypes.DOUBLE,
                               DataTypes.DATE, DataTypes.STRING,
                               DataTypes.ITEM, DataTypes.ANY_SCALAR,
                               DataTypes.DERIVED]
    }

    convertible_types = TYPE_CONV_TABLE[from_type]
    if to_type not in convertible_types:
        conv_types = '", "'.join(name for name, _, _ in convertible_types)
        raise LiteralTypeError(
            f'Type {from_type[0]} cannot be converted to {to_type[0]}. '
            f'Valid conversions are "{conv_types}".')
