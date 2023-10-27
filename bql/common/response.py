# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import json

from .job import Job
from .response_error import UnexpectedResponseError
from .response_util import _check_response
from .single_item_response import SingleItemResponse


class Response(object):
    """The result of executing a BQL Request. The class contains one 
    :class:`SingleItemResponse` object for each data item in the original 
    Request.

    An instance of this class is returned by :meth:`Service.execute`
    as the result of the :class:`Request` execution. 

    .. note::
        :class:`Response` uses `the sequence protocol`_, so you can use it like 
        a list to obtain the :class:`SingleItemResponse` for each data item.

        .. _the sequence protocol: https://docs.python.org/3/c-api/sequence.html

    Example
    --------
    .. code-block:: python
        :linenos:
        :emphasize-lines: 10

        # Setup environment
        import bql
        bq = bql.Service()

        universe = bq.univ.members('INDU Index')
        data_item = bq.data.px_last(fill='prev')

        # Instantiate and execute Request
        request = bql.Request(universe, data_item)
        response = bq.execute(request)
        data = response[0].df()
        data.head()

    """
    _SUPPORTED_BQLSVC_SCHEMA_MAJOR_VERSION = 1
    _SUPPORTED_BQLASVC_SCHEMA_MAJOR_VERSION = 1

    def __init__(self, sirs, raw):
        self._raw = raw
        self._sirs = sirs
        # Lazily evaluated.
        self.__sirs_by_name = None

    @classmethod
    def from_execute_response(cls,
                              json_fragments,
                              execution_context=None,
                              raw=None):
        return cls.__from_json_response(
            json_fragments,
            Response._SUPPORTED_BQLSVC_SCHEMA_MAJOR_VERSION,
            execution_context,
            raw
        )

    @classmethod
    def from_fetch_response(cls,
                            json_fragments,
                            execution_context=None,
                            raw=None):
        return cls.__from_json_response(
            json_fragments,
            Response._SUPPORTED_BQLASVC_SCHEMA_MAJOR_VERSION,
            execution_context,
            raw
        )

    @classmethod
    def __from_json_response(cls,
                             json_fragments,
                             expected_major_version,
                             execution_context,
                             raw):
        json_response = ''.join(json_fragments)
        response = json.loads(json_response)
        _check_response(response, expected_major_version, execution_context)
        if 'ordering' in response:
            # There is ordering information in the response.  First walk
            # through all the raw single item responses, removing duplicates.
            sirs = {resp['name']: resp for resp in response['results'].values()}
            # Obtain the ordering, and sort it by requestIndex.
            ordering = sorted(response['ordering'],
                              key=lambda x: x['requestIndex'])
            # Finally, create the list of raw single item responses in the
            # right order.
            sirs = [sirs[item['responseName']] for item in ordering]
        else:
            # No ordering information: just create a list of single item
            # responses from the dictionary values.
            # TODO(kwhisnant): This seems dangerous if item_names is not None.
            # Maybe we should print a warning?
            sirs = response['results'].values()
        # Create the SingleItemResponse objects, applying the user-supplied
        # item names if given.
        item_names = (execution_context or {}).get('item_names')
        if item_names is None:
            item_names = [None] * len(sirs)
        sirs = [SingleItemResponse(resp, execution_context, name)
                for (name, resp) in zip(item_names, sirs)]
        return cls(sirs, raw)

    def __len__(self):
        return len(self._sirs)

    def __getitem__(self, index):
        return self._sirs[index]

    def _text(self):
        # Return the raw JSON string received from the backend.
        if not self._raw:
            raise ValueError('raw response is not available')
        return self._raw

    def single(self):
        """Returns the only :class:`SingleItemResponse` for a :class:`Response` 
        with a single data item. 
        
        This method will raise an exception if this Response has more than one 
        `SingleItemResponse` object contained within it.

        Returns
        -------
        SingleItemResponse
            The only :class:`SingleItemResponse` object in the 
            :class:`Response`.

        Example
        -------
        .. code-block:: python
            :linenos:
            :emphasize-lines: 13

            # Setup environment
            import bql
            bq = bql.Service()

            last_price = bq.data.px_last(fill='prev')

            # Instantiate and execute Request
            request = bql.Request('AAPL US Equity', last_price)
            response = bq.execute(request)

            # Unpack the data for the only object in Response
            # This is effectively the same as `data = response[0].df()`.
            data = response.single().df()
            data

        """
        if len(self._sirs) == 0:
            raise UnexpectedResponseError('No result received')
        elif len(self._sirs) > 1:
            raise UnexpectedResponseError(
                f'Got {len(self._sirs)} results, expected 1')
        else:
            return self._sirs[0]

    def get(self, name):
        # Return the :class:`SingleItemResponse` by its name.
        if self.__sirs_by_name is None:
            self.__sirs_by_name = {sir.name: sir for sir in self._sirs}
        return self.__sirs_by_name[name]
