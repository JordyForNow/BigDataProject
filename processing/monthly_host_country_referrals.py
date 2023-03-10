from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# Both on the HDFS
datasets_path = "/user/s1710699/websdr-referers"
outfiles_path = 'websdr-country-referrers'

# Since the data is already sorted by month we can save some grouping
for year in range(2014, 2023):
    for month in range(1, 13):

        if year == 2022 and month > 10:
            # No data for end of 2022
            break

        print('Processing: {} {}'.format(year, month))

        dataset_path = datasets_path + "/year=" + str(year) + "/month=" + str(month)

        print(dataset_path)

        df = spark.read.parquet(dataset_path)

        # Parse just the hostname from the referrer field
        # Remove 'Referer: ' prefix
        # NOTE: we use an expression since the function only works with a fixed length
        step1 = expr("substring(request, 10)")
        # Remove protocols and paths
        step2 = split(step1, '/')[2]

        is_ipv6 = step2.startswith('[')

        # Remove ports
        # NOTE: this step breaks ipv6 addresses, so those are processed differently
        step3 = split(step2, ':')[0]

        step3_ipv6 = split(split(step2, ']')[0], '\[')[1]

        hosts_df = df.select(when(is_ipv6, step3_ipv6).otherwise(step3).alias('host'), col('country_iso'))

        host_referrals_df = hosts_df.groupBy(col('host'), col('country_iso')).count()

        outfile_path = outfiles_path + "/year=" + str(year) + "/month=" + str(month)

        # We have a relatively small amount of hosts, so we can coalesce into a single partition
        # to avoid creating many small files on the HDFS
        host_referrals_df.coalesce(1).write.csv(outfile_path)
