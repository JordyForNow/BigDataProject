from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

import csv
# time spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=10 getCountPerDay.py
combinedList = []

spark = SparkSession.builder.getOrCreate()

# Both on the HDFS
datasets_path = "/user/s1710699/websdr-referers"
outfiles_path = 'file://home/s2646587/count_per_day_final.csv'

# Since the data is already sorted by month we can save some grouping
for year in range(2014, 2015):
    for month in range(1, 3):

        if year == 2022 and month > 10:
            # No data for end of 2022
            break

        print('Processing: {} {}'.format(year, month))

        year_folder = "/year=" + str(year)
        month_folder = "/month=" + str(month)
        dataset_path = datasets_path + year_folder + month_folder

        df = spark.read.parquet(dataset_path)
        df2 = df.select(date_format("timestamp", "dd-MM-yyyy").alias("date"))
        df3 = df2.groupBy("date").count()
        df4 = df3.sort("date")
        countedList = df4.collect()

        for i in countedList:
            combinedList.append([i[0], i[1]])

fields = ["Row"]

with open(outfiles_path, 'w') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(fields)
    csv_writer.writerows(combinedList)