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
from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

def create_dummy_df(num_of_records=100):
    Faker.seed(1)
    fake = Faker()

    cols = list(fake.profile().keys())
    profiles = [fake.profile() for i in range(0, num_of_records, 1)]

    print('Columns - {}'.format(cols))
    df = spark.createDataFrame(data=profiles)

    return df

def write_to_folder(df):
    sink_folder = '/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/sample_data'
    df.printSchema()
    df.show(5)

    df.write \
        .option("header", True) \
        .partitionBy(["sex", "blood_group", "created_at"]) \
        .mode("append") \
        .csv(sink_folder)   
    return None

def transform_profile(profile):
    address = profile.address
    created_at = '2021-11-01'
    blood_group = profile.blood_group
    company = profile.company
    job = profile.job
    name = profile.name
    sex = profile.sex
    username = profile.username
    website = " || ".join(profile.website)
    new_profile = (address, blood_group, company, job, name, sex, username, website, created_at)
    return new_profile


def read_data_part(spark):
    # Reading data from partition
    root = '/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/sample_data'
    parition_info = {
        'sex': 'F',
        'blood_group': 'A-',
        'created_at': '2021-11-01'
    }
    keys = list(parition_info.keys())
    vals = [parition_info.get(k) for k in keys]
    pairs = list(zip(keys, vals))
    params = [p[0] + '=' + p[1] for p in pairs]
    path = root + "/" + "/".join(params)
    print('#### \n\nReading from path - {}'.format(path))
    df = spark.read\
                .option("header", "true")\
                .csv(path)
    return df

def test_write_to_partition(spark):
    df = create_dummy_df(num_of_records = 12000)
    cols = ['address', 'blood_group', 'company',
            'job', 'name', 'sex', 'username', 'website', 'created_at']
    df = df.rdd.map(lambda profile: transform_profile(profile)).toDF(cols)
    df.show(3)
    write_to_folder(df)
    return

def test_read_from_partition(spark):
    df = read_data_part(spark)
    df.show(5)
    return

if __name__ == '__main__':
    spark = SparkSession.\
                builder.\
                getOrCreate()
    test_write_to_partition(spark)
    print('#### Creating Checkpoint. #####')
