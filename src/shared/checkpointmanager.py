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
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

class CheckpointInfo:
    def __init__(self):
        self.query_params = {
            'start_dt': None,
            'end_dt': None,
            'brand':None
        }
        self.source = 'MONGO'
        self.sink = 'HDFS'
        self.run_id = None
        self.run_started_at = None
        self.run_status = None
        self.run_end_at = None
        self.task_type = None
    
    def generate_end_dt(self, duration_in_secs = 0):
        self.end_dt = self.start_dt + timedelta(seconds=duration_in_secs)
        return self.end_dt
    
    def update_run_info(self, run_info):
        self.run_id = run_info.get('run_id', None)
        self.run_started_at = run_info.get('run_started_at', None)
        self.run_end_at = run_info.get('run_end_at', None)
        return self
    
    def update_brand_name(self,brand_name):
        self.query_params['brand'] = brand_name
        return self
    
    def update_task_type(self,task_type):
        self.task_type = task_type
        return self
    
    def update_query_params(self, old_ckpt):
        old_end_time = old_ckpt.query_params.get('end_dt', None)
        if not old_end_time:
            raise ValueError('Insufficient argument: end_dt must be present.')

        self.query_params['start_dt'] = old_end_time
        # 1 day long job
        self.query_params['end_dt'] = old_end_time + timedelta(days=1)
        return self

    def update_in_db(self,spark,config):
        print('## Updating checkpoint in DB\n{}'.format(self))
        schema = StructType([
            StructField("source", StringType(), True),
            StructField("sink", StringType(), True),
            StructField("run_started_at", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("run_end_at", StringType(), True),
            StructField("run_status", StringType(), True),
            StructField("task_type", StringType(), True),
            StructField("source_query_param", StringType(), True)
        ])
        data = {
            'source': self.source,
            'sink': self.sink,
            'run_started_at': self.run_started_at.isoformat(),
            'run_end_at': self.run_end_at and self.run_end_at.isoformat(),
            'run_id': str(self.run_id),
            'run_status': self.run_status,
            'task_type' : self.task_type,
            'source_query_param': json.dumps(self.query_params, default=str)
        }
        db_host = glom(config, 'write_config.checkpoint.db_host')
        database = glom(config, 'write_config.checkpoint.database')
        schem = glom(config, 'write_config.checkpoint.schema')
        table= glom(config, 'write_config.checkpoint.table')
        usern= glom(config, 'write_config.checkpoint.username')
        pwd =  glom(config, 'write_config.checkpoint.password')
        driver= glom(config, 'write_config.checkpoint.driver')
        df = spark.createDataFrame([data], schema=schema)
        
        df = spark.createDataFrame([data], schema=schema)
        df.write.format('jdbc')\
            .options(
                url='jdbc:postgresql://{}/{}'.format(db_host,database),
                dbtable='{}."{}"'.format(schem,table),
                user='{}'.format(usern),
                password='{}'.format(pwd),
                driver='{}'.format(driver)
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
            Task Type:{self.task_type}
            Query Param: {self.query_params}

        '''

def read_last_checkpoint_info(spark, config):
    logger.info('## Reading last checkpoint ##')
    db_host = glom(config, 'read_config.checkpoint.db_host')
    database =  glom(config, 'read_config.checkpoint.database')
    schema =  glom(config, 'read_config.checkpoint.schema')
    table =  glom(config, 'read_config.checkpoint.table')
    usern =  glom(config, 'read_config.checkpoint.username')
    pwd = glom(config, 'read_config.checkpoint.password')
    driver = glom(config, 'read_config.checkpoint.driver')
    brands = glom(config,'partition_info.brand')
    task = glom(config,'partition_info.task_type')
    jdbcDF2 = spark.read.format("jdbc").\
    jdbcDF2 = spark.read.format("jdbc").\
        options(
            url='jdbc:postgresql://{}/{}'.format(db_host,database),
            query='select * from {}."{}" where task_type=\'{}\' run_status = \'SUCCESS\' and source_query_param:: json->>\'brand\' =\'{}\' order by run_started_at desc NULLS LAST limit 1'.format(schema,table,brands,task),
            user='{}'.format(usern),
            password='{}'.format(pwd),
            driver='{}'.format(driver)).\
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
    Brand = l_ckpt.query_params['brand']

    l_ckpt.query_params['start_dt'] = datetime.fromisoformat(start_dt)
    l_ckpt.query_params['end_dt'] = datetime.fromisoformat(end_dt)
    l_ckpt.query_params['brand'] = Brand


    l_ckpt.run_id = l_checkpoint_info.run_id
    l_ckpt.run_end_at = datetime.fromisoformat(l_checkpoint_info.run_end_at)
    l_ckpt.run_started_at = datetime.fromisoformat(l_checkpoint_info.run_started_at)
    l_ckpt.run_status = l_checkpoint_info.run_status
    l_ckpt.task_type = l_checkpoint_info.task_type
    logger.info('## Last successful checkpoint details\n{}'.format(l_ckpt))
    return l_ckpt


def prepare_checkpoint_info(l_ckpt_info, run_info, spark,config,curr_ckpt_info=CheckpointInfo(),):
    logger.info('## Preparing current checkpoint object.')
    curr_ckpt_info.update_query_params(l_ckpt_info)
    curr_ckpt_info.update_run_info(run_info)
    curr_ckpt_info.update_brand_name(config)
    return l_ckpt_info, curr_ckpt_info

def prepare_run_info():
    run_info = {
        'run_id': uuid.uuid1(),
        'run_started_at': datetime.now(),
        'run_end_at': None
    }
    return run_info
def prepare_brand_name(config):
    brand_name = glom(config,'partition_info.brand')
    return brand_name

def prepare_task_type(config):
    task_name = glom(config,'partition_info.task_type')
    return task_name
    