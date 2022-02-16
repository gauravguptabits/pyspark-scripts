# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from pyspark.sql import SparkSession
# from pyspark.sql.types import FloatType
import datetime
import itertools
from collections import ChainMap
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, ArrayType, MapType, DoubleType
import reverse_geocoder as rg
import random

__author__ = 'Gaurav Gupta'
spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

catalog = { 
    "brands": [
        {
            "active": True,
            "name": "Sony",
            "categories": ["Consumer Electronics"],
                    "platforms": [
                                {
                                    "name": "Twitter",
                                    "hashtags": ["#Sony", "#sonyindia", "#SonySupport"],
                                    "handle": "Sony"
                                }
            ]
        },
        {
            "active": False,
            "name": "Huawei",
            "categories": ["Consumer Electronics"],
                    "platforms": [
                                {
                                    "name": "Twitter",
                                    "hashtags": ["#Huawei"],
                                    "handle": "Huawei"
                                }
            ]
        },
        {
            "active": True,
            "name": "Panasonic",
            "categories": ["Consumer Electronics"],
                    "platforms": [
                                {
                                    "name": "Twitter",
                                    "hashtags": ["#Panasonic", "#panasonicindia", "#panasonicsupport"],
                                    "handle": "Panasonic"
                                }
            ]
        },
        {
            "name": "LG",
                    "active": True,
            "categories": ["Consumer Electronics"],
                    "platforms": [
                                {
                                    "name": "Twitter",
                                    "hashtags": ["#LG", "#LGLifeIsGood", "#LGLiving"],
                                    "handle": "LG"
                                }
            ]
        }
    ]
}

def get_epoch_time():
    ep = datetime.datetime(1970, 1, 1, 0, 0, 0)
    x = (datetime.datetime.utcnow() - ep).total_seconds()
    return x


def get_active_brands(brand_config):
    brand_config['brands'] = list(
        filter(lambda brand: brand.get('active', False), brand_config['brands']))
    return brand_config


def get_hashtag(brand_config):
    brand = brand_config.get('name', None)
    platforms = brand_config.get('platforms', [])
    twitter = next(filter(lambda p: p.get('name')
                   == 'Twitter', platforms), None)
    return {brand: twitter.get('hashtags')}


tweetSchema = StructType([
    StructField("text", StringType(), True),
    StructField("name", IntegerType(), True),
    # StructField("country", StringType(), True),
    StructField("choice", StringType(), True),
    StructField("place_choice", StringType(), False),
    StructField("created_at", StringType(), True),

    StructField("place", StructType([
        StructField("country_code", StringType(), False)
    ]), False),

    StructField("user", StructType([
        StructField("handle", StringType(), False)
    ]), False),
    StructField("entities", StructType([
        StructField("hashtags", 
                    ArrayType(
                        MapType(
                            StringType(), 
                            StringType(), 
                            False
                        )
                    ), 
                    False
                )
    ])),
    StructField("coordinates", StructType([
        StructField("coordinates", ArrayType(DoubleType(), False))
    ])),
    StructField("retweeted_status", StructType([
        StructField("extended_tweet", StructType([
            StructField("full_text", StringType()),
            StructField("entities", StructType([
                StructField("hashtags", 
                            ArrayType(MapType(StringType(), StringType())),
                        )
            ]))
        ]))
    ]))
    
])


# @F.udf(returnType=IntegerType())
def infer_complaint(text):
    return random.choice([0, 1])


def get_hashtags_from_catalogue(catalog):
    active_brands = get_active_brands(catalog)
    brand_hashtags = dict(ChainMap(
        *[get_hashtag(brand_config) for brand_config in active_brands.get('brands')]))
    all_hashtags = []
    brands_mask = []
    for k, v in brand_hashtags.items():
        all_hashtags.append(v)
        brands_mask.append([k for i in range(len(v))])
    all_hashtags = list(itertools.chain(*all_hashtags))
    all_hashtags = list(map(lambda tag: tag.lower(), all_hashtags))
    brands_mask = list(itertools.chain(*brands_mask))
    return all_hashtags, brands_mask


def find_brand_from_catalog(incoming_hashtags, all_hashtags, brands_mask):
    brand = None

    def exists_in_catalog(hashtag, all_hashtags, brands_mask):
        i = all_hashtags.index(hashtag) if hashtag in all_hashtags else None
        brand = brands_mask[i] if i is not None else None
        return brand

    for htag in incoming_hashtags:
        hashtag = '#' + htag['text'].lower()
        brand = exists_in_catalog(hashtag, all_hashtags, brands_mask)
        if brand:
            break
    return brand


"""
Determine the brand from the hashtags present in the catalog.
"""
def determine_brand(bc_all_hashtags, bc_brands_mask):

    def _determine_brand(incoming_hashtags, rt_hashtags):
        all_hashtags = bc_all_hashtags.value
        brands_mask = bc_brands_mask.value

        incoming_hashtags = incoming_hashtags or []
        rt_hashtags = rt_hashtags or []
        consolidated_hashtags = list({x['text']: x for x in incoming_hashtags + rt_hashtags}.values())
        print(consolidated_hashtags)
        brand = find_brand_from_catalog(consolidated_hashtags, all_hashtags, brands_mask)
        return brand

    return _determine_brand


def to_datetime(d):
    dt = datetime.strptime(d, '%a %b %d %H:%M:%S +0000 %Y')
    return str(datetime.strftime(dt, '%Y-%m-%d %H:%M:%S'))+'.000'

"""
Determine country of origin for the tweet from either coordinates or country_code directly.
"""
def determine_country(coordinates, country_code):
    cc = None
    if coordinates:
        coords = tuple(coordinates)
        results = rg.search(coords) if len(coords) == 2 else [{'cc': None}]
        cc = results[0].get('cc', None)
    
    if country_code:
        cc = country_code
    return cc


if __name__ == '__main__':
    # data = [float(i) for i in np.arange(1, 10000000)]
    # logger.error(f'Total records: {len(data)}')
    # t_before = get_epoch_time()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'localhost:9091') \
        .option("subscribe", 'tweets') \
        .option("startingOffsets", "latest") \
        .load() 
    
    tweetDf = df.select(
                    F.from_json(
                        df.value.cast(StringType()),
                        tweetSchema
                    ).alias('values')
                )
    all_hashtags, brands_mask = get_hashtags_from_catalogue(catalog)
    
    bc_all_hashtags = spark.sparkContext.broadcast(all_hashtags)
    bc_brands_mask = spark.sparkContext.broadcast(brands_mask)

    u_infer_complaint = F.udf(infer_complaint, returnType=IntegerType())
    u_determine_brand = F.udf(determine_brand(bc_all_hashtags, bc_brands_mask), returnType=StringType())
    u_determine_country = F.udf(determine_country, returnType=StringType())
    u_to_datetime = F.udf(to_datetime, returnType=StringType())

    operationalDf = tweetDf.select(
                                    F.col("values.text"),
                                    # F.col("values.place_choice"),
                                    F.col("values.place.country_code"),
                                    F.col("values.entities.hashtags"),
                                    F.col("values.created_at"),
                                    # F.col("values.choice"),
                                    F.col("values.coordinates.coordinates"),
                                    F.col("values.retweeted_status.extended_tweet.entities.hashtags").alias("rt_hashtag")
                                ).\
                                withColumn("Complaint", u_infer_complaint(F.col("text"))).\
                                withColumn("brand", u_determine_brand(F.col("hashtags"), F.col("rt_hashtag"))).\
                                withColumn("country", u_determine_country(
                                    F.col("coordinates"), 
                                    F.col("country_code"))).\
                                withColumn("occurred_on", )

    # query = operationalDf.\
    #             writeStream.\
    #             outputMode("update").\
    #             format("console").\
    #             trigger(continuous='1 second').\
    #             start()

    checkpointDir = "/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/ckpt"
    dataDir = "/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/streaming_data"

    checkpointDir = 'hdfs://172.20.22.198:9820/spark_streaming/ckpt'
    dataDir = 'hdfs://172.20.22.198:9820/spark_streaming/data'

    query = operationalDf.\
        writeStream.\
        outputMode("append").\
        format("parquet").\
        option("path", dataDir).\
        option("checkpointLocation", checkpointDir).\
        start()


    # query = operationalDf.writeStream.outputMode("update").format(
    #     "console").trigger(continuous='1 second').start()
    query.awaitTermination()
