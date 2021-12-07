#from _typeshed import Self
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import urllib
import time
from dotenv import dotenv_values, load_dotenv
import json
import os
from datetime import *
import uuid
from glom import glom
from pyspark.sql import functions as F
from pyspark.sql import udf
import pandas as pd
import json
import urllib
from pathlib import *
from requests.utils import requote_uri
from shared.checkpointmanager import CheckpointInfo,read_last_checkpoint_info,prepare_checkpoint_info,prepare_run_info,prepare_task_type
from os import path
__author__ = 'gaurav'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)


def prepare_query(config):
    schema =  glom(config, 'read_config.checkpoint.schema')
    table =  glom(config, 'read_config.checkpoint.table')
    brands = glom(config,'partition_info.brand')
    task = glom(config,'partition_info.task_type')
    query='select * from {}."{}" where task_type=\'{}\' and run_status = \'SUCCESS\' and source_query_param:: json->>\'brand\' =\'{}\' order by run_started_at desc NULLS LAST limit 1'.format(schema,table,task,brands)
    return query


def _generate_part_file_location(curated_partition_info):
    keys = list(curated_partition_info.keys())
    vals = [curated_partition_info.get(k) for k in keys]
    pairs = list(zip(keys, vals))
    params = [p[0] + '=' + p[1] for p in pairs]
    return "/".join(params)

def _generate_part_files_location(config,ckpt_info):   
    source_location = glom(config, 'read_config.data.folder')
        
    from_dt= ckpt_info.query_params['start_dt']
    till_dt= ckpt_info.query_params['end_dt']

    dates = ['2021-10-24', '2021-10-25','2021-10-26','2021-10-27']
    #dates = list(pd.date_range(from_dt, till_dt).strftime('%Y-%m-%d'))

    c = glom(config,'partition_info.category')
    b = glom(config,'partition_info.brand')
    cpis = [{'category': c, 'brand': b, 'created_at': dt} for dt in dates]
    rel_file_paths = [_generate_part_file_location(cpi) for cpi in cpis]
    abs_file_paths = [source_location+'/'+ rel_file_path for rel_file_path in rel_file_paths]
      
    return abs_file_paths


def read_data_part(spark, path):
    # Reading data from partition
    print('#### \n\nReading from path - \n\n\n{}'.format(path))
    df = spark.read.option("header", "true").json(path)
    return df

    

def transform_df(df):
    # user defined func to extract text field
    
    extract_text_udf = F.udf(lambda row: row.text, StringType())
        # created_at = glom(partition_info, 'created_at')

    df2 = df.withColumn("text", extract_text_udf(col("tweetInfo")))
    df.unpersist()
    df2 = df2.drop(col('tweetInfo'))
        # df2 = df2.withColumn("created_at", F.lit(created_at))
    df2 = df2.withColumn("source", F.lit('Twitter'))
    return df2
def write_to_sink(df, config):
    print("##Writing data to sink")
    folder = glom(config, 'write_config.data.folder')
    brand = glom(config, 'partition_info.brand')
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
    
    run_info = prepare_run_info()
    task_type = prepare_task_type(config)
    #source_sink_name = prepare_source_sink(config)
    curr_ckpt_info = CheckpointInfo()

    try:
        
        l_ckpt_info = read_last_checkpoint_info(spark, config)
       
        l_ckpt_info, curr_ckpt_info = prepare_checkpoint_info(l_ckpt_info, 
                                                            run_info, 
                                                            task_type,
                                                            curr_ckpt_info)

        file_paths = _generate_part_files_location(config,curr_ckpt_info)
        curr_ckpt_info.run_status = 'RUNNING'
        curr_ckpt_info.update_in_db(spark,config)
        for fpath in file_paths:
            if path.exists(fpath):  
                df = read_data_part(spark, fpath)
                df = transform_df(df)
                try:    
                    write_to_sink(df,config)
                    run_info['run_end_at'] = datetime.now()
                    curr_ckpt_info.update_run_info(run_info)
                    df.show()
                    curr_ckpt_info.run_status = 'SUCCESS'
                except:
                    print("=================error")
    except Exception as e:
        logger.error(e)
        curr_ckpt_info.run_status = 'ERROR'
        
    finally:
        curr_ckpt_info.run_end_at = datetime.now()
        curr_ckpt_info.update_in_db(spark,config)   
    return