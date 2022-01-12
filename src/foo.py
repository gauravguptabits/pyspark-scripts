# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from math import e
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
import datetime
import numpy as np

__author__ = 'Gaurav Gupta'
spark = SparkSession.\
    builder.\
    getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)


def get_epoch_time():
    ep = datetime.datetime(1970, 1, 1, 0, 0, 0)
    x = (datetime.datetime.utcnow() - ep).total_seconds()
    return x

def square(partitionData):
    # logger.error(f'Partition type: {type(partitionData)}')
    for row in partitionData:
        yield (row[0], row[0]+2)

def squareImpl2(row):
    return (row[0], row[0]+2, )

if __name__ == '__main__':
    data = [float(i) for i in np.arange(1, 10000000)]
    logger.error(f'Total records: {len(data)}')
    t_before = get_epoch_time()
    df = spark.createDataFrame(data, FloatType())
    df = df.repartition(8)
    df = df.rdd.mapPartitions(square).toDF(['Original', 'Sum(orig, 2)'])
    df.collect()
    t_after = get_epoch_time()
    logger.error(f'Time taken in mapPartition - {t_after - t_before}')
    df.show()

    df2 = spark.createDataFrame(data, FloatType())
    t_before = get_epoch_time()
    df2 = df2.repartition(8)
    df2 = df2.rdd.map(squareImpl2).toDF(['Original', 'Sum(orig, 2)'])
    df2.collect()
    t_after = get_epoch_time()
    logger.error(f'Time taken in map - {t_after - t_before}')
    df2.show()
