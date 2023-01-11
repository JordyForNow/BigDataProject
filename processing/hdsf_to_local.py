from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import pandas
import os

in_hdfs_path = 'websdr-countries'
out_local_path = 'referrer-monthly-countries'

for year in range(2014, 2023):
    for month in range(1, 13):
        if year == 2022 and month > 10:
            # No data for end of 2022
            break
        print('Processing: {} {}'.format(year, month))
        year_folder = "/year=" + str(year)
        month_folder = "/month=" + str(month)
        in_path = in_hdfs_path + year_folder + month_folder

        out_directory = f"{out_local_path}/{str(year)}"
        out_path = f"{out_directory}/{str(year)}-{str(month)}.csv"

        print(in_path, "->", out_path)

        os.makedirs(out_directory, exist_ok=True)

        df = spark.read.csv(in_path)
        pd = df.toPandas()
        pd.to_csv(out_path)
