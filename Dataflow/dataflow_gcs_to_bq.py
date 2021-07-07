"""
python dataflow_gcs_to_bq.py --input_file gs://bbsm-dev-bucket/dataflow/input_dir/video_games_sales.csv --target_bq_table training2021.video_games_sales
--project bbsm-dev --runner DataflowRunner --region us-central1 --temp_location gs://dataflow-staging-us-central1-995870156386
--staging_location gs://dataflow-staging-us-central1-995870156386/

python dataflow_gcs_to_bq.py --input_file gs://bbsm-dev-bucket/dataflow/input_dir/video_games_sales.csv --target_bq_table training2021.video_games_sales --project bbsm-dev
--runner DataflowRunner --region us-central1 --temp_location gs://dataflow-staging-us-central1-995870156386
--staging_location gs://dataflow-staging-us-central1-995870156386/ --setup_file ./setup.py

"""
import argparse
import csv
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions,SetupOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from google.cloud import bigquery
from beamutils.BQDataTypes import BQDataTypes





def get_schema_as_list(project_id, dataset_id, table_id):
    """
    returns schema as [{'field1':value1,'field2':value2},{......}]
    """
    bq_client = bigquery.Client()
    table = bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    table_schema = table.schema
    schema_fields = [{'name': schema_field.name, 'field_type': schema_field.field_type, 'mode': schema_field.mode} for
                     schema_field in table_schema]
    return {'fields': schema_fields}


#Define custom options
class GCSToBQOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_file', help='Input file path (GCS)', required=True)
        parser.add_argument('--target_bq_table', help='Target bigquery table (<dataset_id>.<table_id>', required=True)
        parser.add_argument('--field_delimiter', help='Field delimiter used in the file', default=',')


class StringToRowDict(beam.DoFn):
    def process(self, element, schema):
        fieldnames = [field['name'] for field in schema['fields']]
        fields_dtypes = {field['name']:field['field_type'] for field in schema['fields']}
        reader = csv.DictReader([element], fieldnames=fieldnames)
        row = [r for r in reader][0]
        for k,v in row.items():
            if fields_dtypes[k] in BQDataTypes.numeric_dtypes:
                try:
                    row[k] = int(v)
                except:
                    row[k] = -1
            if fields_dtypes[k] in BQDataTypes.decimal_dtypes:
                try:
                    row[k] = float(v)
                except:
                    row[k] = -1.0
        yield row


#save_main_session is to avoid NameErrors (https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors)
def run(argv=None, save_main_session=True):
    pipeline_options = GCSToBQOptions()
    project_id = pipeline_options.view_as(GoogleCloudOptions).project
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    dataset_id, table_id = pipeline_options.target_bq_table.split('.')
    full_table_id = dataset_id + '.' + table_id
    schema = get_schema_as_list(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    schema_str = ",".join([f"{field['name']}:{field['field_type']}" for field in schema['fields']])
    print(schema_str)
    #p = beam.Pipeline(options=pipeline_options)


    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromInputFile' >> beam.io.ReadFromText(file_pattern=pipeline_options.input_file,
                                                          skip_header_lines=1)
            | 'ConvertStringToRowDict' >> beam.ParDo(StringToRowDict(), schema)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(table=full_table_id,
                                                           schema=schema_str,
                                                           create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                           write_disposition=BigQueryDisposition.WRITE_TRUNCATE)
        )




if __name__ == "__main__":
    run()
