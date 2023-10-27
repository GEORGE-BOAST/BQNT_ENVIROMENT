# Copyright 2018 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import threading

from .bql_item import BqlItem


class _NameAutogenerator:
    """Private class for generating let item names for unnamed let items.

    This class is only used by the let() method in this file.
    """

    def __init__(self):
        self.__counter = 0
        self.__lock = threading.Lock()

    def next_name(self):
        """Return the next auto-generated let item name.

        Returns:
            string: The next auto-generated name.
        """
        with self.__lock:
            i = self.__counter
            self.__counter += 1
        return f"_t{i}"


_name_autogenerator = _NameAutogenerator()


class _LetItem(BqlItem):
    """Class to name BqlItems for ease of use/shorthand notation.

    Args:
        BqlItem (BqlItem): The BqlItem to name.
    """

    def __init__(self, let_name, is_output_column, *args, **kwargs):
        self._let_name = let_name

        # is_output_column is a boolean that's True if and only if this
        # _LetItem was created from another _LetItem with an output_column
        # specified on top of it:
        #
        # OutputColumn(LetItem)
        self._is_output_column = is_output_column

        super(_LetItem, self).__init__(*args, **kwargs)

        # _let_uses is a collection of let items that this item depends on.
        # Before pushing back ourselves into this OrderedDict, we must
        # check that the same let name wasn't already used for something else.
        if (let_name in self._let_uses and not
                self.equals(self._let_uses[let_name])):
            error = f"Let name '{let_name}' used for multiple values"
            raise ValueError(error)

        self._let_uses[let_name] = self

    def __repr__(self):
        cls_name = self.__class__.__bases__[0].__name__
        return (f'<{self.__class__.__name__}({self.name!r}, '
                f'{cls_name}{self._param_string()}){self._out_col_string()}>')

    def __deepcopy__(self, memo_dict):
        cpy = super(_LetItem, self).__deepcopy__(self, memo_dict)
        cpy._let_name = self._let_name
        cpy._is_output_column = self._is_output_column
        return cpy

    def format_for_query_string(
            self,
            expand_items=False,
            store_query_string=False):
        """Returns the BQL query string representation of this Let item.

        This is the method that gets called for:
            * str(let_item)
            * generating the get() clause of a Request()

        Args:
            expand_items (bool, optional): If expand_items is True, it
            returns the fully expanded BQL get() clause (eg: "PX_LAST()").
            If expand_items is False, it returns self.name (eg: "#last").
            Defaults to False.

            store_query_string (bool, optional): Indicates whether to store
            the resulting BQL string in a member for later use.
            Defaults to False.

        Returns:
            string: The BQL string representation of this Let item.
        """
        if expand_items:
            query = super(_LetItem, self).format_for_query_string(
                expand_items,
                store_query_string)
        else:
            query = f'#{self.name}'

            # Add output column specifier.
            #
            # Note: only output_columns add the output_column name here
            #       in the get() clause.  Regular LetItems without an
            #       output_column specified add the output_column name
            #       to the let() clause.  See the assignment() method.
            if self._is_output_column and self._output_column:
                query = f"({query}).{self._output_column}"
        return query

    def _with_outputcol(self, output_column):
        """Returns (creates) a new Let item with an output_column name.

        Args:
            output_column (string): The output column name.

        Returns:
            _LetItem: The new Let item with the output_column name.
        """
        is_output_column = True
        return _LetItem(
            self._let_name,
            is_output_column,
            self._name,
            self._metadata,
            output_column,
            self.positional_parameter_values,
            self.named_parameters
        )

    def with_parameters(self, *args, **kwargs):
        # If the user specifies _retain_let=True, then we want to keep the
        # let() variable assigned to the self object; otherwise, we return
        # a new BqlItem that is not assigned to a let() variable.
        default = False
        retain_let = kwargs.pop('_retain_let', default)
        if retain_let:
            obj = _LetItem(
                self._let_name,
                self.is_output_column,
                self._name,
                self._metadata,
                self._output_column,
                args,
                kwargs
            )
        else:
            obj = super().with_parameters(*args, **kwargs)
        return obj

    def assignment(self):
        """Returns the "#name=expression" assignment string of this Let item.

        This is the method that gets called for each Let item in:
            * generating the let() clause of a Request()
        """
        # For output_column types, which have been created from existing
        # Let items with an output_col name specified, we do not append the
        # output_col name here in the let() clause.  Rather, we append the
        # output_col name only in the get() clause & the str(let_item).
        add_output_column = not self._is_output_column
        expression = super(_LetItem, self).format_for_query_string(
            add_output_column=add_output_column)
        return f"#{self._let_name}={expression};"

    @property
    def name(self):
        return self._let_name

    @property
    def is_output_column(self):
        return self._is_output_column


def let(name, expression):
    """Create a new Let Item (:class:`_LetItem`) with the given ``name`` and 
    ``expression``.

    Let Items are PyBQL's implementation of ``let()``, an optional BQL clause
    that allows you to define local variables in your query. You can use these 
    variables to simplify other clauses.

    Parameters
    ----------
    name : str 
        The name of this Let Item.
    expression : BqlItem 
        The BqlItem to be named.

    Returns
    -------
    _LetItem
        Class to name BqlItems for ease of use or shorthand notation.
    
    Examples
    --------
    .. code-block:: python
        :linenos:
        :emphasize-lines: 5

        # Setup environment
        import bql
        bq = bql.Service()

        data_item = bql.let('marketcap', bq.data.eqy_sh_out() * bq.data.px_last())

        # Instantiate Request and view query string
        request = bql.Request('AAPL US Equity', data_item)
        request.to_string()
    
    """
    if name is None:
        name = _name_autogenerator.next_name()
    e = expression
    return _LetItem(
        let_name=name,
        is_output_column=False,
        name=e._name,
        metadata=e._metadata,
        output_column=e._output_column,
        positional_params=e.positional_parameter_values,
        named_params=e.named_parameters)
