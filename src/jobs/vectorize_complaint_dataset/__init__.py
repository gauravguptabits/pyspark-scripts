import re
from functools import partial
from shared.context import JobContext

__author__ = 'Gaurav Gupta'


def analyze(spark, sc, config):
    print('#####\n\n\nVECTORIZATION DONE\n\n\n#####')
    dept = [("XXXX_Finance", 10), ("XXXX_Marketing", 20), ("XXXX_Sales", 30), ("XXXX_IT", 40)]
    deptColumns = ["dept_name", "dept_id"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    deptDF.printSchema()
    deptDF.show(truncate=False)

    return None
