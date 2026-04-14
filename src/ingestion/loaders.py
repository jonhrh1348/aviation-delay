import json
import tempfile
import os

def load_json_to_spark_df(spark, records):
    json_str = json.dumps(records)
    rdd = spark.sparkContext.parallelize([json_str])
    return spark.read.json(rdd)

def load_json_to_spark_df_windows(spark, records):
    # Create a temporary file to hold the JSON Lines
    # Using delete=False so Windows doesn't lock the file prematurely
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp_path = tmp.name
        # Write each dictionary as its own JSON line
        for record in records:
            tmp.write(json.dumps(record) + '\n')
            
    try:
        # Have the JVM read the file directly (No RDDs involved!)
        df = spark.read.json(tmp_path)
        
        # Force Spark to evaluate the DataFrame before deleting the file
        df.cache().count() 
        return df
        
    finally:
        # Clean up the temp file from your hard drive
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except PermissionError:
                pass # Windows sometimes holds onto the file lock temporarily