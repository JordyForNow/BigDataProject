from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# NOTE: this needs to be run locally with Python 3 on small data
# Ensure pandas is available for easy handling of the small data
import pandas
import os

in_hdfs_path = 'some_hdfs_file.csv'
out_directory = 'results/'
out_local_path = out_directory + 'result.csv'

print(in_hdfs_path, "->", out_local_path)

# Ensure the local path exists
os.makedirs(out_directory, exist_ok=True)

# Read from HDFS
df = spark.read.csv(in_hdfs_path)
# Convert to local pandas dataframe
pd = df.toPandas()
# Write to local system
pd.to_csv(out_local_path)
