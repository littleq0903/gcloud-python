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
    from gcloud import credential
    from gcloud.bigquery.connection import Connection
    # TODO: implement Connection in bigquery.connection

    svc_account_credentials = credentials.get_for_service_account(
        client_email, private_key_path, scope=SCOPE)
    return Connection(credentials=svc_account_credentials)


def get_dataset(dataset_id, client_email, private_key_path):
    connection = get_connection(client_email, private_key_path)
    return connection.dataset(dataset_id)
    # TODO: implement Connection.dataset method

