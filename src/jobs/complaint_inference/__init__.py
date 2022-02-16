import datetime
import pickle
import pandas as pd
from glom import glom
from tabulate import tabulate
from .helper import process_data
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import scrubadub

__author__ = 'Gaurav Gupta'

# table = str.maketrans("", "")

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
        return (row[0], row[1], ans, )

    return _predict


def process_text_interface_fn(bc_lemmatizer, bc_stopwords, bc_email_detector):

    def _process_text_interface_fn(row):
        options = {
            'handle_unicode': True,
            'handle_emoji': True,
            'handle_email': True,
            'handle_username': True,
            'handle_hashtags': True,
            'handle_url': True,
            'handle_markup': True,
            'handle_retweet': False,
            'handle_case': True,
            'handle_lemmatization': True,
            'handle_stopwords': True,
            'handle_punctuation': True,
            'handle_contractions': True,
            'print_stats': False
        }
        pdf = pd.DataFrame(data={'text': [row.text]})
        # print(pdf) 
        pdf = process_data( pdf, 
                            lemmatizer = bc_lemmatizer.value, 
                            stopwords = bc_stopwords.value,
                            emailDetector=bc_email_detector.value,
                            **options
                        )
        orig_text = list(pdf['orig_text'])[0]
        prepped_text = list(pdf['text'])[0]
        # prepped_text = prepped_text .replace('HASHTAG', '').replace('HANDLE', '').replace('URL', '').replace('rt', '')
        return (orig_text, prepped_text,)

    return _process_text_interface_fn
"""
Run scikit learn predictive model to work on distributive load.
"""
def analyze(spark, sc, config):
    # spark = SparkSession.builder.getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    # SATISFY PRE-REQUISITE
    source = glom(config, 'read_config.data.folder')
    sink = glom(config, 'write_config.data.folder')

    vectorizer_path = glom(config, 'read_config.ml_model.vectorizer')
    classifier_path = glom(config, 'read_config.ml_model.classifier')

    num_of_cores = 20
    num_of_partiotions = 3*num_of_cores


    wnl = WordNetLemmatizer()
    stop = stopwords.words('english')
    stop.extend(['panasonic'])
    emailDetector = scrubadub.Scrubber(detector_list=[scrubadub.detectors.EmailDetector])

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
    bc_lemmatizer = spark.sparkContext.broadcast(wnl)
    bc_stopwords = spark.sparkContext.broadcast(stop)
    bc_email_detector = spark.sparkContext.broadcast(emailDetector)

    # PERFORMING ETL
    logger.info(f'Loading data from parquet format...')
    df = spark.read.parquet(source)
    
    # df = df.sample(False, 0.1, 0)
    df = df.repartition(num_of_partiotions)
    t_before = get_epoch_time()

    # Task Initializations
    prediction_mapper_fn = predict(bc_vectorizer, bc_classifier)
    prepped_data_fn = process_text_interface_fn(bc_lemmatizer, 
                                                bc_stopwords, 
                                                bc_email_detector)

    # Task Definition
    df2 = df.rdd.map(prepped_data_fn).toDF(['Orig_text', 'text'])
    df2 = df2.rdd.map(prediction_mapper_fn).toDF(['Orig_text', 'text', 'Complaint'])

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
