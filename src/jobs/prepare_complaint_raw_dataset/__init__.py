from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import urllib
from dotenv import dotenv_values, load_dotenv
import json
import os
from datetime import datetime
import uuid
from glom import glom
from pyspark.sql import functions as F
from pyspark.sql import udf
import pandas as pd
import json
import urllib
from requests.utils import requote_uri

__author__ = 'gaurav'

def _generate_part_file_location(curated_partition_info):
    keys = list(curated_partition_info.keys())
    vals = [curated_partition_info.get(k) for k in keys]
    pairs = list(zip(keys, vals))
    params = [p[0] + '=' + p[1] for p in pairs]
    return "/".join(params)

def _generate_part_files_location(partition_info, config):
    source_location = glom(config, 'read_config.data.folder')
    from_date = glom(partition_info, 'from')
    from_dt = datetime.fromisoformat(from_date)

    till_date = glom(partition_info, 'till')
    till_dt = datetime.fromisoformat(till_date)
    # dates = ['2021-10-24', '2021-10-25']
    dates = list(pd.date_range(from_dt, till_dt).strftime('%Y-%m-%d'))

    c = glom(partition_info, 'category')
    b = glom(partition_info, 'brand')

    cpis = [{'category': c, 'brand': b, 'created_at': dt} for dt in dates]
    rel_file_paths = [_generate_part_file_location(cpi) for cpi in cpis]
    abs_file_paths = [source_location+'/'+ rel_file_path for rel_file_path in rel_file_paths]
    return abs_file_paths

def read_data_part(spark, path):
    # Reading data from partition

    print('#### \n\nReading from path - \n\n\n{}'.format(path))

    df = spark.read.option("header", "true").json(path)
    return df


def transform_df(df, partition_info):
    # user defined func to extract text field
    extract_text_udf = F.udf(lambda row: row.text, StringType())
    # created_at = glom(partition_info, 'created_at')

    df2 = df.withColumn("text", extract_text_udf(col("tweetInfo")))
    df.unpersist()
    df2 = df2.drop(col('tweetInfo'))
    # df2 = df2.withColumn("created_at", F.lit(created_at))
    df2 = df2.withColumn("source", F.lit('Twitter'))
    return df2


def write_to_sink(df, config, partition_info):
    folder = glom(config, 'write_config.data.folder')
    brand = glom(partition_info, 'brand')
    sink_folder = os.path.join(folder, brand)

    df.write \
        .option("header", True) \
        .mode("append") \
        .json(sink_folder)

    return


def analyze(spark, sc, config):
    '''
    TODO: 
        1. [P1] Maintain (Read and write) checkpoint - select * from table where brand == 'XYZ' and status == 'SUCCESS';
        2. [P2] Maintain dataset version. [PARK]
    '''
    partition_info = {
        'category': 'Consumer Electronics',  # get from config
        'brand': 'Amazon',      # get from config
        'from': '2021-10-24',   # get from last checkpoint
        'till': '2021-10-25'    # get from last checkpoint
    }
    file_paths = _generate_part_files_location(partition_info, config)
    df = read_data_part(spark, file_paths)
    df = transform_df(df, partition_info)
    write_to_sink(df, config, partition_info)
    df.show()
    return
