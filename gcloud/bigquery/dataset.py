from gcloud.bigquery.project import Project


class Dataset(object):

    def __init__(self, project, id, connection=None):
        """ A class representing a Dataset on Google BigQuery
        :type project: :class:`gcloud.bigquery.dataset.Dataset`
        :param project: The project which this dataset belongs to
        """
        # connection
        self._connection = connection

        # object-specified arguments
        self._project = project
        self._id = id

    @property
    def connection(self):
        return self._connection

    @property
    def id(self):
        return self._id

    def project(self):
        return Project(self._project)

    @property
    def path(self):
        return '/projects/%s/datasets/%s' % (self._project, self._id)

    @classmethod
    def from_dict(cls, dataset_dict, connection=None):
        reference = dataset_dict['datasetReference']
        project_id = reference['projectId']
        dataset_id = reference['datasetId']
        return cls(connection=connection, project=project_id, id=dataset_id)

    def __repr__(self):
        return '<Dataset: "%s:%s">' % (self._project, self._id)
