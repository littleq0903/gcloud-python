import json
import urllib

from gcloud import connection
from gcloud.bigquery import exceptions
from gcloud.bigquery.iterator import Iterator
from gcloud.bigquery.dataset import Dataset
from gcloud.bigquery.project import Project

class Connection(connection.Connection):
    API_VERSION = 'v2'

    API_URL_TEMPLATE = '{api_base_url}/bigquery/{api_version}{path}'

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)

    def __iter__(self):
        return iter(_ProjectIterator(connection=self))

    def build_api_url(self, path, query_params=None, api_base_url=None,
                      api_version=None):
        url = self.API_URL_TEMPLATE.format(
                api_base_url=(api_base_url or self.API_BASE_URL),
                api_version=(api_version or self.API_VERSION),
                path=path)

        query_params = query_params = {}
        url += '?' + urllib.urlencode(query_params)

        return url

    def make_request(self, method, url, data=None, content_type=None,
                     headers=None):
        """
        refer to cloud storage one.
        """
        headers = headers or {}
        headers['Accept-Encoding'] = 'gzip'

        if data:
            content_length = len(str(data))
        else:
            content_length = 0

        headers['Content-Length'] = content_length

        if content_type:
            headers['Content-Type'] = content_type

        headers['User-Agent'] = self.USER_AGENT

        return self.http.request(uri=url, method=method, headers=headers,
                                 body=data)

    def api_request(self, method, path, query_params=None,
                    data=None, content_type=None,
                    api_base_url=None, api_version=None,
                    expect_json=True):
        """
        refer to cloud storage one.
        """

        url = self.build_api_url(path=path, query_params=query_params,
                                 api_base_url=api_base_url,
                                 api_version=api_version)

        # Making the executive decision that any dictionary
        # data will be sent properly as JSON.
        if data and isinstance(data, dict):
            data = json.dumps(data)
            content_type = 'application/json'

        response, content = self.make_request(
            method=method, url=url, data=data, content_type=content_type)

        if not 200 <= response.status < 300:
            import ipdb; ipdb.set_trace()
            raise Exception('not 200 <= response.status < 300')
            # raise exceptions.make_exception(response, content)

        if content and expect_json:
            content_type = response.get('content-type', '')
            if not content_type.startswith('application/json'):
                raise TypeError('Expected JSON, got %s' % content_type)
            return json.loads(content)

        return content

    def get_all_projects(self):
        return list(self)

    # Dataset getter
    def get_dataset(self, project_id, dataset_id):
        dataset = self.new_dataset(project_id, dataset_id)
        response = self.api_request(method='GET', path=dataset.path)
        return Dataset.from_dict(response, connection=self)

    def new_dataset(self, project_id, dataset_id):
        return Dataset(connection=self, project=project_id, id=dataset_id)


class _ProjectIterator(Iterator):
    def __init__(self, connection):
        super(_ProjectIterator, self).__init__(connection=connection, path='/projects')

    def get_items_from_response(self, response):
        for item in response.get('projects', []):
            yield Project.from_dict(item, connection=self.connection)

