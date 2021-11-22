#from _typeshed import Self
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
from requests.utils import requote_uri

__author__ = 'gaurav'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)


class CheckpointInfo:
    def __init__(self):
        self.query_params = {
            'start_dt': None,
            'end_dt': None
        }
        self.run_id = None
        self.run_started_at = None
        self.run_status = None
        self.run_end_at = None
    
    def generate_end_dt(self, duration_in_secs = 0):
        self.end_dt = self.start_dt + timedelta(seconds=duration_in_secs)
        return self.end_dt
    
    def update_run_info(self, run_info):
        self.run_id = run_info.get('run_id', None)
        self.run_started_at = run_info.get('run_started_at', None)
        self.run_end_at = run_info.get('run_end_at', None)
        return self
    def update_query_params(self, old_ckpt):
        old_end_time = old_ckpt.query_params.get('end_dt', None)
        if not old_end_time:
            raise ValueError('Insufficient argument: end_dt must be present.')

        self.query_params['start_dt'] = old_end_time
        self.query_params['end_dt'] = old_end_time + timedelta(days=4)
        return self
    

    def update_in_db(self,spark,config):
        logger.info('## Updating checkpoint in DB\n{}'.format(self))
        schema = StructType([
            StructField("run_started_at", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("run_end_at", StringType(), True),
            StructField("run_status", StringType(), True),
            StructField("source_query_param", StringType(), True)
            ])
        data = {
            'run_started_at': self.run_started_at.isoformat(),
            'run_end_at': self.run_end_at and self.run_end_at.isoformat(),
            'run_id': str(self.run_id),
            'run_status': self.run_status,
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
            Run ID:{self.run_id}
            Run Start: {self.run_started_at}
            Run End: {self.run_end_at}
            Run Status: {self.run_status}
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
    brand = glom(config,'partition_info.brand')
    jdbcDF2 = spark.read.format("jdbc").\
        options(
            url='jdbc:postgresql://{}/{}'.format(db_host,database),
            query='select * from {}."{}" where run_status=\'SUCCESS\' and brand =\'{}\' order by run_started_at desc NULLS LAST limit 1'.format(schema,table,brand),
            user='{}'.format(usern),
            password='{}'.format(pwd),
            driver='{}'.format(driver)).\
        load()
    jdbcDF2.show()
    l_checkpoint_info = jdbcDF2.collect()[0]
    # convert to checkpoint
    l_ckpt = CheckpointInfo()
    l_ckpt.query_params = json.loads(l_checkpoint_info.source_query_param)
    start_dt = l_ckpt.query_params['start_dt']
    end_dt = l_ckpt.query_params['end_dt']

    l_ckpt.query_params['start_dt'] = datetime.fromisoformat(start_dt)
    l_ckpt.query_params['end_dt'] = datetime.fromisoformat(end_dt)

    l_ckpt.run_id = l_checkpoint_info.run_id
    l_ckpt.run_end_at = datetime.fromisoformat(l_checkpoint_info.run_end_at)
    l_ckpt.run_started_at = datetime.fromisoformat(l_checkpoint_info.run_started_at)
    l_ckpt.run_status = l_checkpoint_info.run_status
    logger.info('## Last successful checkpoint details\n{}'.format(l_ckpt))
    return l_ckpt

def prepare_checkpoint_info(l_ckpt_info, run_info, spark, curr_ckpt_info=CheckpointInfo(),):
    logger.info('## Preparing current checkpoint object.')
    curr_ckpt_info.update_query_params(l_ckpt_info)
    curr_ckpt_info.update_run_info(run_info)
    return l_ckpt_info, curr_ckpt_info

def prepare_run_info():
    run_info = {
        'run_id': uuid.uuid1(),
        'run_started_at': datetime.now(),
        'run_end_at': None
    }
    return run_info
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

    # dates = ['2021-10-24', '2021-10-25']
    dates = list(pd.date_range(from_dt, till_dt).strftime('%Y-%m-%d'))

    c = glom(config,'partition_info.category')
    b = glom(config,'partition_info.brand')

    cpis = [{'category': c, 'brand': b, 'created_at': dt} for dt in dates]
    rel_file_paths = [_generate_part_file_location(cpi) for cpi in cpis]
    abs_file_paths = [source_location+'/'+ rel_file_path for rel_file_path in rel_file_paths]
    return abs_file_paths

def read_data_part(spark, path):
    # Reading data from partition

    logger.info('#### \n\nReading from path - \n\n\n{}'.format(path))

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
    curr_ckpt_info = CheckpointInfo()

    try:
        
        l_ckpt_info = read_last_checkpoint_info(spark, config)
       
        l_ckpt_info, curr_ckpt_info = prepare_checkpoint_info(l_ckpt_info, 
                                                            run_info, spark, 
                                                            curr_ckpt_info)
        
        file_paths = _generate_part_files_location(config,curr_ckpt_info)
        curr_ckpt_info.run_status = 'RUNNING'
        curr_ckpt_info.update_in_db(spark,config)
        df = read_data_part(spark, file_paths)
        df = transform_df(df)
        write_to_sink(df,config)
        run_info['run_end_at'] = datetime.now()
        curr_ckpt_info.update_run_info(run_info)
        df.show()
        curr_ckpt_info.run_status = 'SUCCESS'

    except Exception as e:
        logger.error(e)
        curr_ckpt_info.run_status = 'ERROR'
    finally:
        curr_ckpt_info.run_end_at = datetime.now()
        curr_ckpt_info.update_in_db(spark,config)
    return
