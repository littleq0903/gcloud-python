from gcloud.bigquery.dataset import Dataset
from gcloud.bigquery.iterator import Iterator


class _DatasetIterator(Iterator):
    def __init__(self, project, connection):
        super(_DatasetIterator, self).__init__(connection=connection, path='/projects/%s/datasets' % (project))

    def get_items_from_response(self, response):
        for item in response.get('datasets', []):
            yield Dataset.from_dict(item, connection=self.connection)


class Project(object):
    def __init__(self, id, connection=None):
        # connection
        self._connection = connection

        # object-specified arguments
        self._id = id

    def __iter__(self):
        return iter(_DatasetIterator(connection=self._connection, project=self._id))

    @property
    def connection(self):
        return self._connection

    @property
    def id(self):
        return self._id

    @property
    def path(self):
        return '/projects/%s' % self._id

    @classmethod
    def from_dict(cls, project_dict, connection=None):
        return cls(connection=connection, id=project_dict['id'])

    def __repr__(self):
        return '<Project: %s>' % self._id

    def get_dataset(self, dataset_id):
        return self._connection.get_dataset(project=self._id, id=dataset_id)

    def get_all_datasets(self):
        return list(self)

