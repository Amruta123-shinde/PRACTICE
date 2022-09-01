import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadAllFromBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io import  WriteToText


import argparse
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest="input",
                        required=True,
                        help='please provide the input'
                        )

    # parser.add_argument('--output',
    #                     dest="output",
    #                     required=True,
    #                     help='please provide the input'
    #                     )

    known_args,pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session =True

    def test(element):
        print("===============")
        print(element)
        print("===============")
        return element

    with beam.Pipeline(options=pipeline_options) as p:

        # line = (p | "read" >> ReadFromBigQuery(table=known_args.input)
        #         | "test" >> beam.Map(test)
        #        # |"write" >> WriteToText(known_args.output)
        #         )

        main_table = p | 'readtable' >> beam.io.Read(beam.io.BigQuerySource(query= "select * from  	innate-temple-351706:as_new_dataset.department.department)




# python3 joining_tables.py --input as_new_dataset.department.department --output gs://dataflow_join_table