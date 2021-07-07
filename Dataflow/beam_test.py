import argparse

import  apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions



class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',help='Input file path', default='./test_dir/test_input_file.txt')
        parser.add_argument('--output', help='Output file path', default='./test_dir/test_output')

class CustomDoFn(beam.DoFn):
    def process(self, element, multiplier):
        yield int(element)*multiplier

def run():
    pipeline_options = CustomOptions()
    pipeline_options.view_as(GoogleCloudOptions)
    p = beam.Pipeline(options=pipeline_options)
    #with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        #| beam.Create([1,2,3,4,5])
        | beam.io.ReadFromText(file_pattern=pipeline_options.input)
        #| beam.io.WriteToText(file_path_prefix='./test_dir/output.txt')\
        | beam.ParDo(CustomDoFn(),multiplier=10)
        | beam.io.WriteToText(file_path_prefix=pipeline_options.output)
    )

    p.run()


if __name__ == "__main__":
    run()