import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",
                        dest='input',
                        required=True,
                        help="please provide input gcs path"
                        )
    parser.add_argument("--output",
                        dest="output",
                        required=True,
                        help="please provide output path"

                        )

    # project related arguments
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)

    '''apply the pipeline_arg for current seesion'''

    pipeline_options.view_as(SetupOptions).save_main_session = True

    """below function is for debug only"""


    def test(element):
        print("==========================")
        print(element)
        print("==========================")
        return element


    table_schema = ("word:string,count:integer")


    with beam.Pipeline(options=pipeline_options) as p:
        line = (p | "read" >> ReadFromText(known_args.input)
                # |"test1" >> beam.Map(test)
                )

        sumval = (line | "split" >> beam.FlatMap(lambda x: x.split(","))
                  # | "test" >> beam.Map(test)
                  | "pairwithone" >> beam.Map(lambda x: (x, 1))
                  | "valuesum" >> beam.CombinePerKey(sum)

                  )


        class formatDoFn(beam.DoFn):
            def process(self, element):
                return [{"word": str(element[0]),
                         "count": element[1]}]


        final_output = (sumval | "format" >> beam.ParDo(formatDoFn())
                        | "write" >> WriteToBigQuery(table=known_args.output,
                                                     schema=table_schema,
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition= beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                     )
                        )

# python3 bigqury_save_word_count.py --input gs://as_dataflow_practice_set/dataflow-word-count/input/STUDENT_DATA.csv --output innate-temple-351706:as_dataflow_practice.wordcount1  --runner DataflowRunner --project innate-temple-351706 --temp_location gs://as_dataflow_practice_set/bigqurey/temp/  --staging_location gs://as_dataflow_practice_set/bigquery/stagging/ --region us-east1 --job_name wordcountsavetobigquery
