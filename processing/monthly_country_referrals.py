from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Both on the HDFS
datasets_path = "/user/s1710699/websdr-referers"
outfiles_path = 'websdr-countries'

# Since the data is already sorted by month we can save some grouping
for year in range(2014, 2023):
    for month in range(1, 13):

        if year == 2022 and month > 10:
            # No data for end of 2022
            break

        print('Processing: {} {}'.format(year, month))

        year_folder = "/year=" + str(year)
        month_folder = "/month=" + str(month)
        dataset_path = datasets_path + year_folder + month_folder

        print(dataset_path)

        df = spark.read.parquet(dataset_path)

        # Get just the country of the visit
        countries_df = df.select(col('country_iso'), col('country_name'))

        countries_referrals_df = countries_df.groupBy(col('country_iso'), col('country_name')).count()

        outfile_path = outfiles_path + year_folder + month_folder

        countries_referrals_df.coalesce(1).write.csv(outfile_path)
