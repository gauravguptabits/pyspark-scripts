#!/usr/bin/python
import argparse
import importlib
import time
import os
import sys
from dotenv import dotenv_values, load_dotenv
import json

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from pyspark.sql import SparkSession

__author__ = 'Impressico'

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*', help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()
    print("Called with arguments: %s" % args)
    # TODO: Load config here.
    config = load_config()

    environment = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    }

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print('job_args_tuples: %s' % job_args_tuples)
        job_args = {a[0]: a[1] for a in job_args_tuples}

    print('\nRunning job %s...\nenvironment is %s\n' % (args.job_name, environment))

    # TODO: supply Spark config while creating spark object.
    os.environ.update(environment)
    sparkcon = config.get('spark_conf',None)
    sparkBuilder = SparkSession.builder
    for k,v in sparkcon.items():
        print('key & Values===============',k,v)
        sparkBuilder = sparkBuilder.config(k,v)
    spark = sparkBuilder.getOrCreate()
    sc = spark.sparkContext
    
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    # TODO: Supply Config to the job as argument.
    job_module.analyze(spark, sc,config, **job_args)
    end = time.time()

    print("\nExecution of job %s took %s seconds" % (args.job_name, end-start))
