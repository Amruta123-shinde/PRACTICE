import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
# from apache_beam.io import WriteToBigQuery
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputgcs",
                        dest='inputgcs',
                        required=True,
                        help="please provide input gcs path"
                        )
    parser.add_argument("--outputgcs",
                        dest="outputpath",
                        required=True,
                        help="please provide outputgcs path"

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


    with beam.Pipeline(options=pipeline_options) as p:
        line = (p | "read" >> ReadFromText(known_args.inputgcs)
                # |"test1" >> beam.Map(test)
                )

        sumval = (line | "split" >> beam.FlatMap(lambda x: x.split(","))
                  # | "test" >> beam.Map(test)
                  | "pairwithone" >> beam.Map(lambda x: (x, 1))
                  | "valuesum" >> beam.CombinePerKey(sum)

                  )


        # def format_ele(word,cnt):
        #     return "{} : {} ".format(word,cnt)

        # final_output = (sumval | "format" >> beam.MapTuple(format_ele)
        # |"write" >> WriteToBigQuery(known_args.outputhpath)
        # )

        class formatDoFn(beam.DoFn):
            def process(self, element):
                return "{}: {}".format(element[0], element[1])


        final_output = (sumval | "format" >> beam.ParDo(formatDoFn())
                        | "write" >> WriteToText(known_args.outputpath)
                        )


# python3 bigqury_save_word_count.py --inputgcs gs://as_dataflow_practice_set/input/STUDENT_DATA.csv --outputgcs gs://as_dataflow_practice_set/output --job_name dataflow-practice1 --runner DataflowRunner --project innate-temple-351706 --temp_location gs://as_dataflow_practice_set/temp --staging_location gs://as_dataflow_practice_set/staging --region us-east1


# python3 bigqury_save_word_count.py --inputgcs gs://as_dataflow_practice_set/input/STUDENT_DATA.csv --outputgcs gs://as_dataflow_practice_set/output