from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import urllib
import json
import os
import datetime
import uuid
from main import load_config
from pyspark.sql.functions import lit
from shared.checkpointmanager import CheckpointInfo,read_last_checkpoint_info,prepare_checkpoint_info,prepare_run_info,prepare_task_type
from glom import glom
import traceback

__author__ = 'gaurav'

# logger = logging.getLogger()
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

#==================================== Remote connection setting =======================
def prepare_db_uri(config):
    read_con = config.get('read_config', None)
    username = urllib.parse.quote_plus(config.get('MONGO_USER_NAME'))
    pwd = urllib.parse.quote_plus(config.get('MONGO_USER_PWD'))
    database = read_con.get('data', {}).get('db_name', None)
    collection = read_con.get('data', {}).get('collection')
    host = read_con.get('data', {}).get('db_host', None)
    input_uri = "mongodb://{}/{}".format(host, database)
    return input_uri
#======================================================================================

#===================================== local conncetion setting =======================
#input_uri="mongodb://mongo1:27017"
#database = "Stocks"
#collection = "Source"
#======================================================================================


# the Spark session should be instantiated as follows
# jdbcDF = spark.read.format("jdbc").\
#     options(
#     # jdbc:postgresql://<host>:<port>/<database>
#     url='jdbc:postgresql://localhost:5432/Migration_Catalogue',
#     dbtable='Checkpoint',
#     user='postgres',
#     password='postgres',
#     driver='org.postgresql.Driver').\
#     load()

def read_data_from_source(ckpt_info, config, spark):
    logger.info('####\n\n {}'.format(config))
    input_uri = config.get('input_uri')
    read_con = config.get('read_config', None) 
    database = read_con.get('data',{}).get('db_name')   
    collection = read_con.get('data',{}).get('collection')
    checkpoint = read_con.get('checkpoint_info')

    # convert your date string to datetime object
    start = ckpt_info.query_params['start_dt']
    end = ckpt_info.query_params['end_dt']
    agg_query = [{
        '$match': {
            'tweetInfo.created_at': {
                '$gte': {
                    "$date": start.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                '$lt': {
                    "$date": end.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            }
        }
    }]
    logger.info('#####Mongo Query####\n\n\n{}\n\n'.format(agg_query))
    df = spark.read.format("mongo")\
            .option("uri", input_uri)\
            .option("database", database)\
            .option("sampleSize", 100000)\
            .option("samplePoolSize", 100000)\
            .option("collection", collection)\
            .option("pipeline", agg_query)\
            .load()
    return df

def transform_data(data):
    return data

def copy_data_to_sink(df, sink_options, curr_ckpt_info):
    # TODO: prevent entry of duplicate into the HDFS.
    sink_folder = sink_options.get('sink_folder', None)
    df.select(
            col('brand'), 
            col('category'), 
            col("created_at"),
            col("tweetInfo")) \
        .write \
        .option("header", True) \
        .partitionBy(["category","brand","created_at"]) \
        .mode("append") \
        .json(sink_folder)
    return df


def prepare_query(config):
    schema =  glom(config, 'read_config.checkpoint.schema')
    table =  glom(config, 'read_config.checkpoint.table')
    task = glom(config,'partition_info.task_type')
    query='select * from {}."{}" where task_type=\'{}\' and run_status = \'SUCCESS\' order by run_started_at desc NULLS LAST limit 1'.format(schema,table,task)
    return query

def analyze(spark, sc, config):
    # Prepare meta-data.
    logger.info('## Loading configuration ##')
    input_uri = prepare_db_uri(config)
    config.update({'input_uri': input_uri})
    write_con = config.get('write_config',None)
    sink_folder = write_con.get('data', {}).get('folder', None)
    sink_options = {'sink_folder': sink_folder}
    run_info = prepare_run_info(config)
    task_type = prepare_task_type(config)
    curr_ckpt_info = CheckpointInfo()
    logger.info("Source to Sink: {} --> {}".format(input_uri, sink_folder))
 
    try:
        query = prepare_query(config)
        #print("------------------------",query)
        l_ckpt_info = read_last_checkpoint_info(spark, config,query)
        l_ckpt_info, curr_ckpt_info = prepare_checkpoint_info(l_ckpt_info, 
                                                            run_info, spark, task_type,
                                                            curr_ckpt_info)
        curr_ckpt_info.run_status = 'RUNNING'
        curr_ckpt_info.update_in_db(spark,config)
        # TODO: Do your task here.
        logger.info('Fetching full load data from Mongo')
        df = read_data_from_source(curr_ckpt_info, config, spark)
        # count = df.count()
        # print('## Number of records to copy - {}'.format(count))
        data_for_date = curr_ckpt_info.query_params.get('end_dt').date()
        df = df.withColumn("created_at", lit(data_for_date))
        copy_data_to_sink(df, sink_options, curr_ckpt_info)
        run_info['run_end_at'] = datetime.datetime.now()
        curr_ckpt_info.update_run_info(run_info)
        logger.info('## Printing dataframe sample.')
        df.show(5)
        curr_ckpt_info.run_status = 'SUCCESS'
    except Exception as ex:
        strace = ''.join(traceback.format_exception(etype=type(ex),
                                                    value=ex,
                                                    tb=ex.__traceback__))
        logger.error(strace)
        curr_ckpt_info.run_status = 'ERROR'
        # TODO: Ensure copying wasn't done. If it did, then rollback.
        curr_ckpt_info.run_status = 'ERROR'
    finally:
        curr_ckpt_info.run_end_at = datetime.datetime.now()
        curr_ckpt_info.update_in_db(spark,config)
    logger.info("xxxxxxxxxx Exiting from script xxxxxxxxxxxxx")
    return 
