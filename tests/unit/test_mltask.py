import pandas as pd
from pandas.testing import assert_frame_equal
from dataxml.tasks.mltask import MLTask
from pyspark.sql.types import IntegerType
from delta import DeltaTable


def test_load_data(spark):
    test_ml_config = {"input": {"database": "default", "table": "sample_dataset"}}

    db = test_ml_config["input"]["database"]
    table = test_ml_config["input"]["table"]
    _data = {"a": [1, 2], "b": [3, 4]}
    data = spark.createDataFrame(pd.DataFrame(_data))
    expected_df = pd.DataFrame(_data)
    data.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table}")

    ml_job = MLTask(spark, test_ml_config)
    df = ml_job.load_data()
    assert_frame_equal(df, expected_df)
