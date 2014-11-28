class Dataset(object):
    def __init__(self, id, connection=None):
        self._connection = connection
        self._id = id

    def connection(self):
        return self._connection

    def id(self):
        return self._id

    @classmethod
    def from_dict(cls, dataset_dict, connection=None):
        return cls(connection=connection, id=dataset_dict['id'])

    def __repr__(self):
        return '<Dataset: %s>' % self._id
