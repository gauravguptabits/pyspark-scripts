import re
from functools import partial
from shared.context import JobContext

__author__ = 'Gaurav Gupta'


def analyze(spark, sc, config):
    print('#####\n\n\nDE-NOISING DONE\n\n\n#####')
    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)
            ]
    deptColumns = ["dept_name", "dept_id"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    deptDF.printSchema()
    deptDF.show(truncate=False)
    return None
