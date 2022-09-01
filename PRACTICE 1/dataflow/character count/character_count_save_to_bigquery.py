import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText

import logging
import argparse

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("--input",
                        dest='input',
                        required=True,
                        help="please provide the input"
                        )

    parser.add_argument("--output",
                        dest="output",
                        required=True,
                        help="please provide the output"

                        )

    known_arg, pipeline_arg = parser.parse_known_args()


    def test(element):
        print("=============")
        print(element)
        print("==============")
        return test


    table_schema = ("word:string ,char_count:integer")

    pipeline_options = PipelineOptions(pipeline_arg)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        line = (p | "read" >> ReadFromText(known_arg.input)
                )

        val = (line | "split" >> beam.FlatMap(lambda x: x.split(" "))
               | "count_of_char" >> beam.Map(lambda x: (x, len(x)))
               #    |"test" >> beam.Map(test)
               )


        class formatDoFn(beam.DoFn):
            def process(self, element):
                yield {"word": str(element[0]),
                       "char_count": element[1]}


        final = (val | "format" >> beam.ParDo(formatDoFn())
                 | "write" >> beam.io.WriteToBigQuery(table=known_arg.output,
                                                      schema=table_schema,
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

                                                      )
                 )

        #python3 character_count_save_to_bigquery.py --input gs://as-practice-bucket/input/CHARACTER_COUNT.txt --output innate-temple-351706:as_dataflow_practice.charcount --temp_location gs://as_dataflow_practice_set/bigqurey/temp1/ --staging_location gs://as_dataflow_practice_set/bigquery/stagging1/ --region us-east1 --job_name charcountsavetobigquery --project innate-temple-351706

