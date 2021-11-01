#!/usr/bin/python
import argparse
import importlib
import time
import os
import sys

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*', help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()
    print("Called with arguments: %s" % args)
    # TODO: Load config here.

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
    spark = SparkSession.\
                builder.\
                config("spark.mongodb.input.partitioner", "MongoSamplePartitioner").\
                config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "128").\
                config("spark.mongodb.input.partitionerOptions.partitionKey", "brand").\
                getOrCreate()
    sc = spark.sparkContext
    
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    # TODO: Supply Config to the job as argument.
    job_module.analyze(spark, sc, **job_args)
    end = time.time()

    print("\nExecution of job %s took %s seconds" % (args.job_name, end-start))
