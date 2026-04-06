import json

def load_json_to_spark_df(spark, records):
    json_str = json.dumps(records)
    rdd = spark.sparkContext.parallelize([json_str])
    return spark.read.json(rdd)