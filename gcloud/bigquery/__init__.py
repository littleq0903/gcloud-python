"""Shortcut methods

>>> from gcloud import bigquery
>>> dataset = bigquery.get_dataset('dataset-id-here',
...                                'project-id-here',
...                                'long-email@googleapis.com',
...                                '/path/to/private.key')
>>> query
"""

__version__ = '0.1.2'

SCOPE = (
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/bigquery.insertdata',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/devstorage.read_only',
    'https://www.googleapis.com/auth/devstorage.read_write'
)


def get_connection(client_email, private_key_path):
    from gcloud import credentials
    from gcloud.bigquery.connection import Connection

    svc_account_credentials = credentials.get_for_service_account(
        client_email, private_key_path, scope=SCOPE)
    return Connection(credentials=svc_account_credentials)


def get_project(project_id, client_email, private_key_path):
    connection = get_connection(client_email, private_key_path)
    return connection.get_project(project_id)


def get_dataset(dataset_id, project_id, client_email, private_key_path):
    connection = get_connection(client_email, private_key_path)
    return connection.get_dataset(project_id, dataset_id)
