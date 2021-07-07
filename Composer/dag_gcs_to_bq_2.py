from datetime import  datetime, timedelta
import ast

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator


# PROJECT_ID = "bbsm-dev"
# SOURCE_BUCKET_NAME = "bbsm-dev-bucket"
# ARCHIVE_BUCKET_NAME = "bbsm-dev-bucket"
# TARGET_BUCKET_NAME = "bbsm-dev-bucket"
# SOURCE_FILES = ['csv_data/liquor_sales.csv']
# #SOURCE_SCHEMA_FILE = f"gs://{SOURCE_BUCKET_NAME}/csv_data/liquor_data_schema.json"
# SOURCE_SCHEMA_FILE = f"csv_data/liquor_data_schema.json"
# STAGING_DATASET = "training2021"
# STAGING_TABLE = "staging_liquor_sales"
# TARGET_DATASET = "training2021"
# TARGET_TABLE = "liquor_sales_analysis"


#https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/index.html#airflow.models.BaseOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['satyamanib4u@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'CSV_GCS_TO_BQ',
    default_args=default_args,
    description='Load CSV file from GCS to BQ',
    schedule_interval="00 06 * * *",
    start_date=datetime.now(),
    tags=['gcs_to_bq']
)


start = DummyOperator(task_id='start',dag=dag)

#https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/gcs_to_bigquery/index.html
load_csv_to_bq_staging = GCSToBigQueryOperator(
                                    task_id='load_csv_to_bq_staging',
                                    bucket="{{ dag_run.conf['SOURCE_BUCKET_NAME'] }}",
                                    source_objects="{{ dag_run.conf['SOURCE_FILES'] }}".strip('][').split(','),
                                    destination_project_dataset_table="{{ dag_run.conf['STAGING_DATASET'] + '.' + dag_run.conf['STAGING_TABLE'] }}",
                                    #schema_fields="{{ dag_run.conf['SOURCE_SCHEMA'] }}".strip('][').split(','),
                                    create_disposition='CREATE_IF_NEEDED',
                                    skip_leading_rows=1,
                                    write_disposition='WRITE_TRUNCATE',
                                    field_delimiter=',',
                                    autodetect=True,
                                    dag=dag
)
#https://stackoverflow.com/questions/64895696/airflow-xcom-pull-only-returns-string
# def my_func(ds, **kwargs):
#     source_bucket_name = "{{ dag_run.conf['SOURCE_BUCKET_NAME'] }}"
#     #print(f"SOURCE_BUCKET_NAME: {source_bucket_name}")
#     #source_files = eval("{{ dag_run.conf['SOURCE_FILES'] }}")
#     source_files = "{{ dag_run.conf['SOURCE_FILES'] }}".strip('][').split(',')
#     destination_table = "{{ dag_run.conf['STAGING_DATASET'] + '.' + dag_run.conf['STAGING_TABLE'] }}"
#     #source_schema = eval("{{ dag_run.conf['SOURCE_SCHEMA'] }}")
#     source_schema = "{{ dag_run.conf['SOURCE_SCHEMA'] }}".strip('][').split(',')
#     op = GCSToBigQueryOperator(
#                 task_id='load_csv_to_bq_staging',
#                 bucket=source_bucket_name,
#                 source_objects=source_files,
#                 destination_project_dataset_table=destination_table,
#                 schema_fields=source_schema,
#                 create_disposition='CREATE_IF_NEEDED',
#                 skip_leading_rows=1,
#                 write_disposition='WRITE_TRUNCATE',
#                 field_delimiter=',',
#                 dag=dag
#     )
#     op.execute(**kwargs)
#
#
# p = PythonOperator(task_id='python_task', provide_context=True, python_callable=my_func)



load_from_staging_to_target = BigQueryInsertJobOperator(
                                    task_id='load_from_staging_to_target',
                                    configuration={
                                        "query": {
                                            "query":"{{ dag_run.conf['TRANSFORMATION_QUERY'] }}",
                                            "useLegacySql": False
                                        }
                                    },
                                    dag=dag
)


drop_staging_table = BigQueryDeleteTableOperator(
                                    task_id='drop_staging_table',
                                    #deletion_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}',
                                    deletion_dataset_table="{{ dag_run.conf['PROJECT_ID']  + '.' + dag_run.conf['STAGING_DATASET'] + '.' + dag_run.conf['STAGING_TABLE'] }}",
                                    dag=dag
)


archive_source_file = GCSToGCSOperator(
                                    task_id='archive_source_file',
                                    source_bucket="{{ dag_run.conf['SOURCE_BUCKET_NAME'] }}",
                                    source_objects="{{ dag_run.conf['SOURCE_FILES'] }}",
                                    destination_bucket="{{ dag_run.conf['ARCHIVE_BUCKET_NAME'] }}",
                                    destination_object='csv_data_archive/',
                                    move=True,
                                    dag=dag
)

export_target_table_from_bq_to_gcs = BigQueryToGCSOperator(
                                        task_id='export_target_table_from_bq_to_gcs',
                                        #source_project_dataset_table=f'{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}',
                                        source_project_dataset_table="{{ dag_run.conf['PROJECT_ID'] + '.' + dag_run.conf['TARGET_DATASET'] + '.' + dag_run.conf['TARGET_TABLE'] }}",
                                        destination_cloud_storage_uris=["{{ 'gs://' + dag_run.conf['TARGET_BUCKET_NAME'] + '/csv_data/exported_liquor_analysis.csv' }}"]
)


end = DummyOperator(task_id='end',dag=dag)


start >> load_csv_to_bq_staging >> load_from_staging_to_target >> \
[archive_source_file,drop_staging_table,export_target_table_from_bq_to_gcs] >> end