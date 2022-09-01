import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import  logging


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument("--inputgcs",
                        dest='inputgcs',
                        required=True,
                        help="please provide input gcs path"
                        )
    parser.add_argument("--outputgcs",
                        dest="outputhpath",
                        required=True,
                        help="please provide outputgcs path"

                        )

    # project related arguments
    known_args, pipeline_args = parser.parse_known_args()

    '''to initialize pipeline_args which contains the details of project like
    project id,runner,job_name ,temp_location & for this we import pipelineoptions'''

    pipeline_options = PipelineOptions(pipeline_args)

    '''apply the pipeline_arg for current seesion'''

    pipeline_options.view_as(SetupOptions).save_main_session =True


    """below function is for debug only"""

    def test(element):
        print("==========================")
        print(element)
        print("==========================")


    '''create/constructing the pipeline object as p using with clause'''
    """>>  -operations """
    """1st line show the pipeline object creation as p
    below are the transformation we pass --it create the pcollection and each line acts as pcollection
    line is a variable useD--after that split of data by comma separate ,then converting
     each element as tuple & give value as one by using map function
     then use of combinebykey to shuffle data across pcollection sum will return the no of time the occurnace of element
     it will combine it using combine by key"""


    with beam.Pipeline(options = pipeline_options) as p:

        line = p | "read" >> ReadFromText(known_args.inputgcs)

        sumval = (line | "split" >> beam.FlatMap(lambda x:x.split(","))
                  | "pairwithone" >> beam.Map(lambda x:(x,1))
                 | "valuesum" >> beam.CombinePerKey(sum)
                 | "test" >> beam.Map(test)

                 )
        """for the fomating the output in tuple form"""

        def format_ele(word,cnt):
            return "{} {}".format(word,cnt)

        final=(sumval| "format" >>beam.MapTuple(format_ele())
               |"write" >> WriteToText(known_args.outputhpath)
               )

        """transfer into gcs bucket"""



#python3 word_count.py --inputgcs gs://as_dataflow_practice_set/dataflow-word-count/input/STUDENT_DATA.csv --outputgcs gs://as_dataflow_practice_set/output/ --runner DataflowRunner --project innate-temple-351706 --temp_location gs://as_dataflow_practice_set/temp/  --staging_location gs://as_dataflow_practice_set/stagging/ --region us-east1 --job_name wordcount_saveto_gcs