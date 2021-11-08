# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, MapType
import datetime
import uuid
import json
from pyspark.sql.functions import col, lit
from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql import udf

"""
Objective:

1. Read the paritioned data from HDFS.
2. Pulls out the tweeted text to form the dataset.
"""


def read_data_part(spark):
    # Reading data from partition
    root = '/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/datalake'
    parition_info = {
        'category': 'Consumer Electronics',
        'brand': 'Kenstar',
        'created_at': "2021-10-24"
    }
    keys = list(parition_info.keys())
    vals = [parition_info.get(k) for k in keys]
    pairs = list(zip(keys, vals))
    params = [p[0] + '=' + p[1] for p in pairs]
    path = root + "/" + "/".join(params)
    print('#### \n\nReading from path - {}'.format(path))
    df = spark.read\
                .option("header", "true")\
                .json(path)
    return df


def test_read_from_partition(spark):
    df = read_data_part(spark)
    df.printSchema()
    df.show(5)
    return df

def upperCase(row):
    return row.text

upper_udf = F.udf(lambda z: upperCase(z), StringType())

if __name__ == '__main__':
    spark = SparkSession.\
                builder.\
                getOrCreate()
    df = test_read_from_partition(spark)
    df2 = df.withColumn("text", upper_udf(col("tweetInfo")))
    df2[['text']].show()
    print('#### Creating Checkpoint. #####')
