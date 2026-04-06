# src/utils/spark.py
from pyspark.sql import SparkSession

def get_spark(app_name: str = "aviation-delay") -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Asia/Singapore")
    return spark