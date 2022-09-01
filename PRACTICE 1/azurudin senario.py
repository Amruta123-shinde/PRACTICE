from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == '__main__':
    spark = SparkSession.builder.appName('practice set').getOrCreate()

    # # data = [(('amruta', "girish", "kadam"), 1000, 'F'), (('dhiraj', 'shivaji', 'SHINDE'), 1000, 'M'),
    # #         (('komal', '', "patil"), 1000, 'M')]
    # #
    #
    # data = [("'amruta', 'girish', 'kadam' ", 1000, 'F'),( "'dhiraj', 'shivaji', 'SHINDE'", 1000, 'M'),\
    #         ("'komal', , 'patil'", 1000, 'M')]
    #
    #
    # rdd=spark.sparkContext.parallelize(data)
    # # print(rdd.collect())
    #
    # column= ['name','salary','gender']
    # df=rdd.toDF(column)
    # df.show(truncate=False)
    #
    # df1= df.withColumn('first_name',split(col("name"),',').getItem(0))\
    #     .withColumn('middle_name',split(col("name"),',').getItem(1))\
    #     .withColumn("last_name",split(col("name"),',').getItem(2)).drop(col("name"))
    # df1.show()


#===================================================================================
from pyspark.sql.types import *

s=StructType([StructField("name",StringType()),\
              StructField("name",IntegerType()),\
              StructField("name",IntegerType())
              ]
             )

