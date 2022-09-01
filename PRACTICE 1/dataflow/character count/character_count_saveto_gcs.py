import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

import argparse
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='please provide the input file'
                        )

    parser.add_argument("--output",
                        dest='output',
                        required=True,
                        help='please provide the output path'
                        )

    known_arg, pipeline_arg = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_arg)
    pipeline_options.view_as(SetupOptions).save_main_session = True


    def test(element):
        print("==================")
        print(element)
        print("==================")
        return element


    with beam.Pipeline(options=pipeline_options) as p:
        line = p | "read" >> ReadFromText(known_arg.input)

        val = (line | "split" >> beam.FlatMap(lambda x: x.split(" "))
               | "mapping" >> beam.Map(lambda x: (x,len(x))
               )
               )

        class formatDoFn(beam.DoFn):
            def process(self,element):
                return "{} : {} ".format(element[0],element[1])

        final_output = (val | "format" >> beam.ParDo(formatDoFn())
                        | "write" >> WriteToText(known_arg.output)
                        )

#python3 character_count.py --input gs://as-practice-bucket/input/CHARACTER_COUNT.txt --output gs://as-practice-bucket/output/ --temp_location gs://as-practice-bucket/temp/ --staging_location gs://as-practice-bucket/staging/ --runner DataflowRunner --project innate-temple-351706 --region us-east1 --job_name charactercount



