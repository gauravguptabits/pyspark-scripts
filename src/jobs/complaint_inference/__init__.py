import datetime
import pickle
import pandas as pd
from pyspark.sql import SparkSession
from glom import glom
from tabulate import tabulate

__author__ = 'Gaurav Gupta'

# bc_vectorizer, bc_classifier = 1, 2

def get_epoch_time():
    ep = datetime.datetime(1970, 1, 1, 0, 0, 0)
    x = (datetime.datetime.utcnow() - ep).total_seconds()
    return x


def load_model(vec_path, clf_path):

    # pickle.dump(tfidf, open("./models/tfidf_rf_f1_9371.pickle", "wb"))
    # dump(clf, './models/rf_f1_9371.joblib')
    # experiment_path = f"{root}/experiments/{experiment_name}"
    vectorizer = pickle.load(open(vec_path, "rb"))
    clf = pickle.load(open(clf_path, 'rb'))
    model_dict = {'classifier': clf, 'vectorizer': vectorizer}
    return model_dict


def prediction_service(sample_text, vectorizer, clf):
    sample_df = pd.DataFrame(data={'text': [sample_text]})
    # sample_df = process_data(sample_df, print_stats=False)
    sample_vector = vectorizer.transform(sample_df['text'])
    ans = clf.predict(sample_vector)
    sample_df['Prediction'] = ans
    return sample_df


def predict(bc_vectorizer, bc_classifier):

    def _predict(row):
        tfidf = bc_vectorizer
        clf = bc_classifier
        prediction = prediction_service(row.text, vectorizer=tfidf.value, clf=clf.value)
        ans = prediction['Prediction'].loc[0].item()
        return (row.text, ans, )

    return _predict

"""
Run scikit learn predictive model to work on distributive load.
"""
def analyze(spark, sc, config):
    global bc_classifier, bc_vectorizer
    # spark = SparkSession.builder.getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    # SATISFY PRE-REQUISITE
    source = glom(config, 'read_config.data.folder')
    sink = glom(config, 'write_config.data.folder')

    vectorizer_path = glom(config, 'read_config.ml_model.vectorizer')
    classifier_path = glom(config, 'read_config.ml_model.classifier')

    num_of_cores = 8
    num_of_partiotions = 5*num_of_cores

    log_metrics = [
        ('Key', 'Value'), 
        ('Script', __path__),
        ('Source', source), 
        ('Sink', sink), 
        ('Vect', vectorizer_path), 
        ('Clf', classifier_path),
        ('Partition Count', num_of_partiotions)
    ]
    logger.info(f'\n{tabulate(log_metrics)}')
    logger.info(f'Loading ML model...')

    model_dict = load_model(vectorizer_path, classifier_path)
    logger.info(f"Vectorizer: {type(model_dict['vectorizer'])}")
    logger.info(f"Classifier: {type(model_dict['classifier'])}")
    bc_vectorizer = spark.sparkContext.broadcast(model_dict['vectorizer'])
    bc_classifier = spark.sparkContext.broadcast(model_dict['classifier'])

    # PERFORMING ETL
    logger.info(f'Loading data from parquet format...')
    df = spark.read.parquet(source)
    
    df = df.sample(False, 0.1, 0)
    df = df.repartition(num_of_partiotions)
    t_before = get_epoch_time()
    prediction_mapper_fn = predict(bc_vectorizer, bc_classifier)
    rdd2 = df.rdd.map(prediction_mapper_fn)
    df2 = rdd2.toDF(['text', 'Complaint'])

    logger.info(f'Writing back to disk...')
    t_before = get_epoch_time()
    df2\
        .repartition(num_of_partiotions)\
        .write\
        .option("header", True) \
        .mode('overwrite')\
        .parquet(sink)
    t_after = get_epoch_time()
    logger.error(f'Time taken in writing - {t_after - t_before} sec.')

    return {"status": "SUCCESS"}
