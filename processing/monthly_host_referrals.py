from pyspark.sql.functions import col, expr, date_trunc, split
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# TODO fix IPV6 [2001:67c:2564:532:250:daff:fe6e:c945] parsing to [2001

datasets_path = "/user/s1710699/websdr-referers"
outfiles_path = 'websdr-referers'

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

        # Parse just the hostname from the referrer field
        hosts_df = df.select(split(split(expr("substring(request, 10)"), '/')[2], ':')[0].alias('host'))

        host_referrals_df = hosts_df.groupBy(col('host')).count()

        outfile_path = outfiles_path + year_folder + month_folder

        host_referrals_df.coalesce(1).write.csv(outfile_path)
