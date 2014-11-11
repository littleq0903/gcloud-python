import json
import urllib

from gcloud import connection

class Connection(connection.Conection):
    API_VERSION = 'v2'

    API_URL_TEMPLATE = '{api_base_url}/bigquery/{api_version}/projects/{project}{path}'

    def __init__(self, project, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.project = project

    def build_api_url(self, path, query_params=None, api_base_url=None,
                      api_version=None):
        url = self.API_URL_TEMPLATE.format(
                api_base_url=(api_base_url or self.API_BASE_URL),
                api_version=(api_version or self.API_VERSION),
                path=path)

        query_params = query_params = {}
        query_params.update({'project': self.project})
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
            raise exceptions.make_exception(response, content)

        if content and expect_json:
            content_type = response.get('content-type', '')
            if not content_type.startswith('application/json'):
                raise TypeError('Expected JSON, got %s' % content_type)
            return json.loads(content)

        return content

        

