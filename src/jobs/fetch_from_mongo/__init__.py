from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import urllib
from dotenv import dotenv_values, load_dotenv
import json
import os
import datetime
import uuid
from pyspark.sql.functions import lit

__author__ = 'gaurav'

def load_config():
    cwd = os.getcwd()
    path_config_env = os.path.join(cwd, "config", "fetch_from_mongo", "config.env")
    path_config_json = os.path.join(cwd, "config", "fetch_from_mongo", "config.json")

    load_dotenv(str(path_config_env))
    MONGO_USER_NAME = os.getenv('MONGO_USER_NAME')
    MONGO_USER_PWD = os.getenv('MONGO_USER_PWD')

    with open(str(path_config_json)) as f:
        config = json.load(f)

    config.update({
        'MONGO_USER_NAME': MONGO_USER_NAME,
        'MONGO_USER_PWD': MONGO_USER_PWD
    })
    return config

#==================================== Remote connection setting =======================
def prepare_db_uri(config):

    username = urllib.parse.quote_plus(config.get('MONGO_USER_NAME'))
    pwd = urllib.parse.quote_plus(config.get('MONGO_USER_PWD'))
    database = config.get('database_config', {}).get('db_name', None)
    collection = config.get('database_config', {}).get('collection')
    host = config.get('database_config', {}).get('db_host', None)
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


class CheckpointInfo:
    def __init__(self):
        self.query_params = {
            'start_dt': None,
            'end_dt': None
        }
        self.source = 'MONGO'
        self.sink = 'HDFS'
        self.run_id = None
        self.run_started_at = None
        self.run_status = None
        self.run_end_at = None
    
    def generate_end_dt(self, duration_in_secs = 0):
        self.end_dt = self.start_dt + datetime.timedelta(seconds=duration_in_secs)
        return self.end_dt
    
    def update_run_info(self, run_info):
        self.run_id = run_info.get('run_id', None)
        self.run_started_at = run_info.get('run_started_at', None)
        self.run_end_at = run_info.get('run_end_at', None)
        return self
    
    def update_query_params(self, old_ckpt):
        # old_ckpt.query_params = {
        #     'end_dt': datetime.datetime.fromisoformat("2021-10-20T00:00:00.000000")
        # }
        old_end_time = old_ckpt.query_params.get('end_dt', None)
        if not old_end_time:
            raise ValueError('Insufficient argument: end_dt must be present.')

        self.query_params['start_dt'] = old_end_time
        # 1 day long job
        self.query_params['end_dt'] = old_end_time + datetime.timedelta(seconds=24*60*60)
        return self

    def update_in_db(self, spark):
        print('## Updating checkpoint in DB\n{}'.format(self))
        schema = StructType([
            StructField("source", StringType(), True),
            StructField("sink", StringType(), True),
            StructField("run_started_at", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("run_end_at", StringType(), True),
            StructField("run_status", StringType(), True),
            StructField("source_query_param", StringType(), True)
        ])
        data = {
            'source': self.source,
            'sink': self.sink,
            'run_started_at': self.run_started_at.isoformat(),
            'run_end_at': self.run_end_at and self.run_end_at.isoformat(),
            'run_id': str(self.run_id),
            'run_status': self.run_status,
            'source_query_param': json.dumps(self.query_params, default=str)
        }
        df = spark.createDataFrame([data], schema=schema)
        df.write.format('jdbc')\
            .options(
                url='jdbc:postgresql://localhost:5432/Migration_Catalogue',
                dbtable='migration_checkpoints."Checkpoint"',
                user='postgres',
                password='postgres',
                driver='org.postgresql.Driver'
        ).mode('append').save()
        
        return
    
    def __str__(self):
        return f'''
            Source:{self.source}
            Sink:{self.sink}
            Run ID:{self.run_id}
            Run Start: {self.run_started_at}
            Run End: {self.run_end_at}
            Run Status: {self.run_status}
            Query Param: {self.query_params}
        '''

def read_last_checkpoint_info(spark):
    print('## Reading last checkpoint ##')
    jdbcDF2 = spark.read.format("jdbc").\
        options(
            url='jdbc:postgresql://localhost:5432/Migration_Catalogue',
            query='select * from migration_checkpoints."Checkpoint" where run_status=\'SUCCESS\' order by run_started_at desc NULLS LAST limit 1',
            user='postgres',
            password='postgres',
            driver='org.postgresql.Driver').\
        load()
    jdbcDF2.show()
    l_checkpoint_info = jdbcDF2.collect()[0]
    # convert to checkpoint
    l_ckpt = CheckpointInfo()
    l_ckpt.source = l_checkpoint_info.source
    l_ckpt.sink = l_checkpoint_info.sink
    l_ckpt.query_params = json.loads(l_checkpoint_info.source_query_param)
    start_dt = l_ckpt.query_params['start_dt']
    end_dt = l_ckpt.query_params['end_dt']

    l_ckpt.query_params['start_dt'] = datetime.datetime.fromisoformat(start_dt)
    l_ckpt.query_params['end_dt'] = datetime.datetime.fromisoformat(end_dt)

    l_ckpt.run_id = l_checkpoint_info.run_id
    l_ckpt.run_end_at = datetime.datetime.fromisoformat(l_checkpoint_info.run_end_at)
    l_ckpt.run_started_at = datetime.datetime.fromisoformat(l_checkpoint_info.run_started_at)
    l_ckpt.run_status = l_checkpoint_info.run_status
    print('## Last successful checkpoint details\n{}'.format(l_ckpt))
    return l_ckpt


def prepare_checkpoint_info(l_ckpt_info, run_info, spark, curr_ckpt_info=CheckpointInfo(),):
    print('## Preparing current checkpoint object.')
    curr_ckpt_info.update_query_params(l_ckpt_info)
    curr_ckpt_info.update_run_info(run_info)
    return l_ckpt_info, curr_ckpt_info

def read_data_from_source(ckpt_info, config, spark):
    print('####\n\n {}'.format(config))
    input_uri = config.get('input_uri')
    database_config = config.get('database_config', {})
    database = database_config.get('db_name')
    collection = database_config.get('collection')
    checkpoint = database_config.get('checkpoint_info')

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
    print('#####Mongo Query####\n\n\n{}\n\n'.format(agg_query))
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
        .partitionBy(["category", "brand", "created_at"]) \
        .mode("append") \
        .json(sink_folder)
    return df

def prepare_run_info():
    run_info = {
        'run_id': uuid.uuid1(),
        'run_started_at': datetime.datetime.now(),
        'run_end_at': None
    }
    return run_info

def analyze(spark, sc):
    # Prepare meta-data.
    print('## Loading configuration ##')
    config = load_config()
    input_uri = prepare_db_uri(config)
    config.update({'input_uri': input_uri})
    sink_folder = config.get('sink', {}).get('folder', None)
    sink_options = {'sink_folder': sink_folder}
    run_info = prepare_run_info()
    curr_ckpt_info = CheckpointInfo()
    print("Source to Sink: {} --> {}".format(input_uri, sink_folder))
    try:
        l_ckpt_info = read_last_checkpoint_info(spark)
        l_ckpt_info, curr_ckpt_info = prepare_checkpoint_info(l_ckpt_info, 
                                                            run_info, spark, 
                                                            curr_ckpt_info)
        curr_ckpt_info.run_status = 'RUNNING'
        curr_ckpt_info.update_in_db(spark)
        # TODO: Do your task here.
        print('Fetching full load data from Mongo')
        df = read_data_from_source(curr_ckpt_info, config, spark)
        # count = df.count()
        # print('## Number of records to copy - {}'.format(count))
        data_for_date = curr_ckpt_info.query_params.get('end_dt').date()
        df = df.withColumn("created_at", lit(data_for_date))
        copy_data_to_sink(df, sink_options, curr_ckpt_info)
        run_info['run_end_at'] = datetime.datetime.now()
        curr_ckpt_info.update_run_info(run_info)
        print('## Printing dataframe sample.')
        df.show(5)
        curr_ckpt_info.run_status = 'SUCCESS'
    except Exception as e:
        # TODO: Ensure copying wasn't done. If it did, then rollback.
        print(e)
        curr_ckpt_info.run_status = 'ERROR'
    finally:
        curr_ckpt_info.run_end_at = datetime.datetime.now()
        curr_ckpt_info.update_in_db(spark)
    print("xxxxxxxxxx Exiting from script xxxxxxxxxxxxx")
    return 
