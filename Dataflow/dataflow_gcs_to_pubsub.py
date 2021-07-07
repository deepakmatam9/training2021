import argparse
import csv
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions,SetupOptions
from apache_beam.transforms import window

class GCSToPubsubOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_file', help='Input file path (GCS)', required=True)
        parser.add_argument('--topic', help='Pubsub topic to send data to', required=True)



def run(argv=None, save_main_session=True):
    pipeline_options = GCSToPubsubOptions()
    project_id = pipeline_options.view_as(GoogleCloudOptions).project
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    #topic = f"/topics/{project_id}/{pipeline_options.topic}"
    topic = f"projects/{project_id}/topics/{pipeline_options.topic}"
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromInputFile' >> beam.io.ReadFromText(file_pattern=pipeline_options.input_file,
                                                          skip_header_lines=1)
            | 'ConvertStrToBytes' >> beam.Map(lambda x: x.encode('utf-8'))
            | 'WriteToPubsub' >> beam.io.WriteToPubSub(topic=topic)

        )


if __name__ == "__main__":
    run()
