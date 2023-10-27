import re


class RelativeDate(object):
    """Represents a date relative to the current date.

    A relative date is composed of an amount and a unit. The unit can be
    years ('Y'), semi-annuals ('S'), quarters ('Q'), months ('M'),
    weeks ('W'), or days ('D'). The amount is a positive or negative integer
    specifying the offset to the current date in the given unit.
    """

    _regex = None

    def __init__(self, amount, unit):
        UNITS = ('Y', 'S', 'Q', 'M', 'W', 'D')

        unit = unit.upper()
        if unit not in UNITS:
            raise ValueError(
                f'Unit "{unit}" for relative dates is not supported. '
                f'Must be one of {", ".join(UNITS)}')

        self.unit = unit
        self.amount = amount

    @classmethod
    def from_string(cls, val):
        """Parse a string of the form "-52D" into a relative date object."""
        if cls._regex is None:
            cls._regex = re.compile(
                r'^(?P<sign>[+-]?)\s*(?P<amount>[0-9]+)\s*(?P<unit>[YSQMWDysqmwd])$')

        match = cls._regex.match(val)
        if not match:
            raise ValueError(f'"{val}" does not denote a relative date')

        amount = int(match.group('amount'))
        if match.group('sign') == '-':
            amount = -amount
        unit = match.group('unit')

        return RelativeDate(amount, unit)

    def __str__(self):
        return f'{self.amount}{self.unit}'

    def __hash__(self):
        return hash((self.unit, self.amount))

    def __eq__(self, other):
        return (isinstance(other, RelativeDate) and
                self.unit == other.unit and self.amount == other.amount)

    def __ne__(self, other):
        return not self.__eq__(other)
