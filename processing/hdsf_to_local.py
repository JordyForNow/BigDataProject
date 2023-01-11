from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# NOTE: this needs to be run locally with Python 3 on small data
# Ensure pandas is available for easy handling of the small data
import pandas
import os

in_hdfs_path = 'websdr-countries'
out_local_path = 'referrer-monthly-countries'

for year in range(2014, 2023):
    for month in range(1, 13):
        if year == 2022 and month > 10:
            # No data for end of 2022
            break

        in_path = f"{in_hdfs_path}/year={year}/month={month}"

        out_directory = f"{out_local_path}/{year}"
        out_path = f"{out_directory}/{year}-{month}.csv"

        print(in_path, "->", out_path)

        # Ensure the local path exists
        os.makedirs(out_directory, exist_ok=True)

        # Read from HDFS
        df = spark.read.csv(in_path)
        # Convert to local pandas dataframe
        pd = df.toPandas()
        # Write to local system
        pd.to_csv(out_path)
