# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, MapType
import datetime
import uuid
import json
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from random import randrange

__author__ = 'Gaurav Gupta'
root = '/home/gaurav.gupta/projects/PoCs/brandMention/brand_ml'
experiment_name = 'RF_FBeta_8424_Dec31'
bc_vectorizer, bc_classifier = None, None


def get_epoch_time():
    ep = datetime.datetime(1970, 1, 1, 0, 0, 0)
    x = (datetime.datetime.utcnow() - ep).total_seconds()
    return x

def load_model(experiment_name):

    # pickle.dump(tfidf, open("./models/tfidf_rf_f1_9371.pickle", "wb"))
    # dump(clf, './models/rf_f1_9371.joblib')
    experiment_path = f"{root}/experiments/{experiment_name}"
    vectorizer = pickle.load(open(f"{experiment_path}/vectorizer.pickle", "rb"))
    clf = pickle.load(open(f"{experiment_path}/classifier.pickle", 'rb'))
    model_dict = {'classifier': clf, 'vectorizer': vectorizer}
    return model_dict


def tfidf_to_text(vectors, tfidf):
    list_of_tokens = tfidf.inverse_transform(vectors)
    return list(map(lambda tokens: ' '.join(tokens), list_of_tokens))


def read_data(spark):
    sample_text = [("This is such a poor product. It didnot worked for even 1 day",),
                   ("#panasonicindia I twitted again and again but yet my complaint is not solved",),
                   ("@PanasonicIndia Ive a 32 LCD/LED Model No. TH-W32ES48DX in which I'm unable to see Netflix under the Market Apps section. How do I fix this?",),
                   ("Disposed my laptop successfully with the help of @PanasonicIndia #DiwaliwaliSafai #PanasonicIndia Join @chidambar08 @dayalojha_ @AswaniJaishree",),
                   ("Panasonic's Akhil Sethi joins Harman India as digital marketing head #HarmanIndia #Panasonic #AkhilSethi #Vivo #Isobar #Devices #PanasonicIndia",),
                   ]

    cols = ['text']
    df = spark.createDataFrame(data=sample_text, schema=cols)
    return df


def prediction_service(sample_text, vectorizer, clf):

    sample_df = pd.DataFrame(data={'text': [sample_text]})
    # sample_df = process_data(sample_df, print_stats=False)
    sample_vector = vectorizer.transform(sample_df['text'])
    ans = clf.predict(sample_vector)
    sample_df['Prediction'] = ans
    return sample_df


def predict(row):
    tfidf = bc_vectorizer
    clf = bc_classifier
    prediction = prediction_service(row.text, vectorizer=tfidf.value, clf=clf.value)
    ans = prediction['Prediction'].loc[0].item()
    return (row.text, ans, )

if __name__ == '__main__':
    spark = SparkSession.\
                builder.\
                getOrCreate()

    local_source_folder = '/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/Panasonic_parquet'
    local_sink_folder = '/home/gaurav.gupta/projects/PoCs/brandMention/pyspark-scripts/data/Panasonic_predict'
    num_of_cores = 20
    num_of_partiotions = 5*num_of_cores
    remote_source_folder = 'hdfs://172.20.22.198:9820/new_datasets/Panasonic'

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info(f'Starting to read.')

    model_dict = load_model(experiment_name)
    bc_vectorizer = spark.sparkContext.broadcast(model_dict['vectorizer'])
    bc_classifier = spark.sparkContext.broadcast(model_dict['classifier'])

    t_before_read = get_epoch_time()
    df = spark.read.parquet(remote_source_folder)
    num_of_records = df.count()
    t_after_read = get_epoch_time()
    logger.error(f'Time: {t_after_read - t_before_read}\t#Records: {num_of_records}')

    df = df.repartition(num_of_partiotions)
    t_before = get_epoch_time()
    rdd2 = df.rdd.map(predict)
    df2 = rdd2.toDF(['text', 'Complaint'])

    t_before = get_epoch_time()
    df2\
        .repartition(num_of_partiotions)\
        .write\
        .option("header", True) \
        .mode('overwrite')\
        .parquet(local_sink_folder)
    t_after = get_epoch_time()
    logger.error(f'Time taken in writing - {t_after - t_before} sec.')
