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
from pyspark.sql.functions import col


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

    def update_in_db(self, spark):
        QueryParamType = MapType(StringType(), StringType())
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
            'run_end_at': self.run_end_at.isoformat(),
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
    
def test_checkpoint_writing(spark):
    ckpt = CheckpointInfo()
    ckpt.source = 'SPARK'
    ckpt.sink = 'HDFS'
    ckpt.run_id = uuid.uuid1()
    ckpt.run_status = 'SUCCESS'
    ckpt.run_started_at = datetime.datetime.now()
    ckpt.run_end_at = ckpt.run_started_at + datetime.timedelta(seconds=600)
    ckpt.query_params = {
        'start_dt': datetime.datetime.now(),
        'end_dt': datetime.datetime.now()
    }
    print('#### Updating in DB #####')
    ckpt.update_in_db(spark)
    print('#### END of SCRIPT #####')
    return

def read_from_mongo(spark):
    input_uri = 'mongodb://localhost:27017/bigdata'
    database = 'bigdata'
    collection = 'twitterStreamData'
    
    start = datetime.datetime(2021, 10, 22, 0, 0, 0)
    end = datetime.datetime(2021, 10, 23, 0, 0, 0)

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
    # .option("inferSchema", "true")\
    # .option("partitioner", "MongoPaginateBySizePartitioner")\

    print('#####Mongo Query####\n\n\n{}\n\n'.format(agg_query))
    df = spark.read.format("mongo")\
        .option("uri", input_uri)\
        .option("database", database)\
        .option("sampleSize", 50000)\
        .option("collection", collection)\
        .option("pipeline", agg_query)\
        .load()
    return df


def record_iterator(iterator):
    _ids = [rec.tweetInfo.text for rec in iterator]
    # print(_ids)
    print('###\n\nTotal items in partition: {}\n\n###'.format(len(_ids)))
    print(_ids[0:10])

if __name__ == '__main__':
    spark = SparkSession.\
                builder.\
                config("spark.mongodb.input.partitioner", "MongoSamplePartitioner").\
                config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "128").\
                config("spark.mongodb.input.partitionerOptions.partitionKey", "brand").\
                getOrCreate()
    df = read_from_mongo(spark)
    print('## Num of docs: {}'.format(df.count()))
    df.show(4)
    # df.foreachPartition(record_iterator)
    df.select(
        col('brand'),
        col('category'),
        col('req_id'),
        col("tweetInfo").cast('string')).show()
    print('#### Creating Checkpoint. #####')
