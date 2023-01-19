from pyspark.sql.functions import *

###
# hostname
###

# Parse just the hostname from the referrer field
# Remove 'Referer: ' prefix
# NOTE: we use an expression since the function only works with a fixed length
step1 = expr("substring(request, 10)")
# Remove protocols and paths
step2 = split(step1, '/')[2]
# Remove ports
# NOTE: this step breaks ipv6 addresses, but since there are only a few those can be fixed later
#       logic that could parse both would be far more complex to run
step3 = split(step2, ':')[0]

hosts_df = df.select(step3.alias('host'))

host_referrals_df = hosts_df.groupBy(col('host')).count()

###
# de-amp
###
amp_selector = col('host').endswith('cdn.ampproject.org')

amp_step1 = expr("substring(host, 1, length(host)-19)")

amp_step2 = regexp_replace(amp_step1, '--', '/')

amp_step3 = regexp_replace(amp_step2, '-', '.')

amp_step4 = regexp_replace(amp_step3, '/', '-')

de_amp = when(amp_selector, amp_step4).otherwise(col('host'))

de_amped_df = host_referrals_df.select(de_amp.alias('host'), col('count'))

###
# string operations
###
string_step1 = lower(col('host'))
# Remove 'www.' prefix
string_step2 = regexp_replace(string_step1, r'^www.', '')
# Remove '.' suffix
string_step3 = regexp_replace(string_step2, r'\.$', '')
