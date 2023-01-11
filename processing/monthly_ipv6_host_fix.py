from pyspark.sql.functions import col, expr, date_trunc, split, concat, slice
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

datasets_path = "/user/s1710699/websdr-referers"
outfiles_path = 'ipv6-websdr-referers'

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
        # remove referer prefix
        step1 = expr("substring(request, 10)")
        # keep online website
        step2 = split(step1, '/')[2]

        # test6 = '[2001:67c:2564:532:250:daff:fe6e:c945]:5001'.split(']')[0].split('[')
        # test4 = 'google.com:1234'.split(']')[0].split('[')
        # (test6[0].split(':')[:1] + test6[1:])[-1]
        # (test4[0].split(':')[:1] + test4[1:])[-1]

        ipv6_hosts_df = df.select(step2.alias('host')).where(col('host').startswith('['))
        # remove ports
        step3 = split(split(step2, ']')[0], '\[')[1]

        hosts_df = df.select(step3.alias('host')).where(col('host') != '')

        host_referrals_df = hosts_df.groupBy(col('host')).count()

        outfile_path = outfiles_path + year_folder + month_folder

        host_referrals_df.coalesce(1).write.csv(outfile_path)
