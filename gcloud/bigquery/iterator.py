

class Iterator(object):
    # TODO: move this Iterator and storage.iterator.Iterator to up level

    PAGE_TOKEN = 'pageToken'
    RESERVED_PARAMS = frozenset([PAGE_TOKEN])

    def __init__(self, connection, path, extra_params=None):
        self.connection = connection
        self.path = path
        self.page_number = 0
        self.next_page_token = None
        self.extra_params = extra_params or {}

        reserved_in_use = self.RESERVED_PARAMS.intersection(self.extra_params)
        if reserved_in_use:
            raise ValueError(('Using a reserved parameter', reserved_in_use))

    def __iter__(self):
        """Iterate through the list of items."""
        while self.has_next_page():
            response = self.get_next_page_response()
            for item in self.get_items_from_response(response):
                yield item

    def has_next_page(self):
        """Determines whether or not this iterator has more pages.
        :rtype: bool
        :returns: Whether the iterator has more pages or not.
        """
        if self.page_number == 0:
            return True

        return self.next_page_token is not None

    def get_query_params(self):
        """Getter for query parameters for the next request.
        :rtype: dict
        :returns: A dictionary of query parameters.
        """
        result = ({self.PAGE_TOKEN: self.next_page_token}
                  if self.next_page_token else {})
        result.update(self.extra_params)
        return result

    def get_next_page_response(self):
        """Requests the next page from the path provided.
        :rtype: dict
        :returns: The parsed JSON response of the next page's contents.
        """
        if not self.has_next_page():
            raise RuntimeError('No more pages. Try resetting the iterator.')

        response = self.connection.api_request(
            method='GET', path=self.path, query_params=self.get_query_params())

        self.page_number += 1
        self.next_page_token = response.get('nextPageToken')

        return response

    def reset(self):
        """Resets the iterator to the beginning."""
        self.page_number = 0
        self.next_page_token = None

    def get_items_from_response(self, response):
        """Factory method called while iterating. This should be overriden.
        This method should be overridden by a subclass.  It should
        accept the API response of a request for the next page of items,
        and return a list (or other iterable) of items.
        Typically this method will construct a Bucket or a Key from the
        page of results in the response.
        :type response: dict
        :param response: The response of asking for the next page of items.
        :rtype: iterable
        :returns: Items that the iterator should yield.
        """
        raise NotImplementedError
