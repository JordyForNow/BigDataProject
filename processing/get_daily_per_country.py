from pyspark.sql.functions import dayofmonth

datasets_path = "/user/s1710699/websdr-referers"
year = "2017"
month = "12"

dataset_path = datasets_path + "/year=" + str(year) + "/month=" + str(month)
df = spark.read.parquet(dataset_path)

df2 = df.select(dayofmonth("timestamp").alias("date"), "country_name", "city")
df3 = df2.filter(df2.country_name == "United States")
df4 = df3.select("date", "city")
df5 = df4.groupBy("city", "date").count()
df6 = df5.sort("city", "date")