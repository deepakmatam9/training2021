from google.cloud import bigquery
from google.cloud import storage
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

class CustomEnvVariables:
    DEFAULT_PROJECT_ID = "bbsm-dev"
    DEFAULT_DATASET_ID = "training2021"
    DEFAULT_BUCKET = "bbsm-dev-bucket"
    DEFAULT_TRANSFORMATION_QUERIES_PATH = "gs://bbsm-dev-bucket/transformation_queries/"
    CLIENT_ID = "1077736373304-lsorsnik9t69tssk15aqvm04t8hlp9m0.apps.googleusercontent.com"
    #WEBSERVER_ID = "https://g482e22ccee24b617p-tp.appspot.com"
    WEBSERVER_ID = "https://g482e22ccee24b617p-tp.appspot.com"
    IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
    OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
    DAG_NAME = "CSV_GCS_TO_BQ"



bq_client = bigquery.Client()
gcs_client = storage.Client()


def get_schema_as_list(project_id, dataset_id, table_id):
    table = bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    table_schema = table.schema
    schema_fields = [{'name':schema_field.name,'field_type':schema_field.field_type,'mode':schema_field.mode} for  schema_field in table_schema]
    return schema_fields


def get_transformation_query(source_file):
    transformation_filename = source_file.split('/')[-1].split('.')[0]+ '_transformation.sql'
    transformation_filepath = CustomEnvVariables.DEFAULT_TRANSFORMATION_QUERIES_PATH + transformation_filename
    print(f"Transformation filename: {transformation_filepath}")
    bucket_name = CustomEnvVariables.DEFAULT_TRANSFORMATION_QUERIES_PATH.replace('gs://','').split('/')[0]
    object_prefix = ''.join(CustomEnvVariables.DEFAULT_TRANSFORMATION_QUERIES_PATH.replace('gs://','').split('/')[1:])
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.get_blob(object_prefix + '/' + transformation_filename)
    query = blob.download_as_string().decode("utf-8")
    return query


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text
# END COPIED IAP CODE



def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload. https://cloud.google.com/storage/docs/json_api/v1/objects#resource
         context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None; the output is written to Stackdriver Logging
    """
    source_bucket = event['bucket']
    source_file = event['name']
    staging_table_name = 'staging_' + source_file.split('/')[-1].split('.')[0]
    target_table_name = source_file.split('/')[-1].split('.')[0] + '_analysis'
    print(f"Staging table: {staging_table_name} and Target table: {target_table_name}")
    staging_table_schema = get_schema_as_list(CustomEnvVariables.DEFAULT_PROJECT_ID,
                                              CustomEnvVariables.DEFAULT_DATASET_ID,
                                              staging_table_name)

    print(f"Staging table schema {staging_table_schema}")


    transformation_query = get_transformation_query(source_file)
    transformation_query = transformation_query.format(STAGING_DATASET=CustomEnvVariables.DEFAULT_DATASET_ID,
                                                       STAGING_TABLE=staging_table_name,
                                                       TARGET_DATASET=CustomEnvVariables.DEFAULT_DATASET_ID,
                                                       TARGET_TABLE=target_table_name)

    print(f"Transformationv query: {transformation_query}")

    webserver_url = CustomEnvVariables.WEBSERVER_ID + '/api/experimental/dags/' + CustomEnvVariables.DAG_NAME + '/dag_runs'

    data = {
            "PROJECT_ID": CustomEnvVariables.DEFAULT_PROJECT_ID,
            "SOURCE_BUCKET_NAME":source_bucket,
            "ARCHIVE_BUCKET_NAME":CustomEnvVariables.DEFAULT_BUCKET,
            "TARGET_BUCKET_NAME": CustomEnvVariables.DEFAULT_BUCKET,
            "SOURCE_FILES": [source_file],
            "SOURCE_SCHEMA": staging_table_schema,
            "STAGING_DATASET": CustomEnvVariables.DEFAULT_DATASET_ID,
            "STAGING_TABLE": staging_table_name,
            "TARGET_DATASET": CustomEnvVariables.DEFAULT_DATASET_ID,
            "TARGET_TABLE": target_table_name,
            "TRANSFORMATION_QUERY": transformation_query
    }

    make_iap_request(webserver_url, CustomEnvVariables.CLIENT_ID, method='POST', json={"conf": data, "replace_microseconds": 'false'})








