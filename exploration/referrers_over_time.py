import math
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from matplotlib import pyplot as plt
import matplotlib as mpl
import numpy as np

in_folder = '../Data/referrer-monthly-host-countries_processed'

###
# Coalesce the data for all months into a single dataframe
###

monthly_dataframes = []

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path)

            country_count_df = df[['host', 'count']].groupby('host').sum().reset_index()
            country_count_df['year'] = year
            country_count_df['month'] = month

            monthly_dataframes.append(country_count_df)

all_months = pd.concat(monthly_dataframes)

# Shift the indexes so rows are months and columns are referrers
result = all_months.pivot(index=['year', 'month'], values='count', columns='host')

# Remove the websdr itself, as it takes up the bulk of referrals and is not very interesting
result_no_websdr = result.drop('websdr.ewi.utwente.nl', axis=1)

# Get traffic per referrer for the whole duration of the data set
hosts_all_time = result_no_websdr.sum().astype('int').sort_values(ascending=False)

# Sort on total amount of referrals made
sorted_result_no_websdr = result_no_websdr[hosts_all_time.index]

# Turn this into percentages using the total amount of visits in the data set
all_time_visits = hosts_all_time.sum()
hosts_all_time_percentage = hosts_all_time / all_time_visits * 100

###
# Aggregate smallest referrers, as they are not interesting individually
###
topn = 250

print('Top', topn, 'make up', hosts_all_time_percentage[:topn].sum(), '% of traffic')

top_result_no_wbsdr = sorted_result_no_websdr.iloc[:, :topn]

rest_result = sorted_result_no_websdr.iloc[:, topn:].sum(axis=1)
top_result_no_wbsdr['REST'] = rest_result

all_visits_per_month = top_result_no_wbsdr.sum(axis=1)

# Turn this into percentages using the total amount of visits
top_result_no_wbsdr_dist = top_result_no_wbsdr.div(all_visits_per_month, axis=0)

###
# Pieplot of the distribution of traffic over referrers
###
# hosts_all_time_labels = hosts_all_time.reset_index()['host']
# top_hosts_all_time_labels = hosts_all_time.reset_index()['host']
# top_hosts_all_time_labels[20:] = None
#
# pieplot = hosts_all_time.plot.pie(labels=top_hosts_all_time_labels)
# plt.show()

###
# Look at referrer distribution over time
###
years_labels = result.index.to_series().apply(lambda x : f"{x[0]}" if x[1] == 1 else None)

# Find in how many months each referrer occurs
emptiness = top_result_no_wbsdr_dist.count().sort_values(ascending=False)

# Sort first on the amount of months a referrer occurs in, then alltime referrals
emptiness.name = 'emptiness'
hosts_all_time.name = 'hat'
sort_key = pd.concat([emptiness, hosts_all_time], axis=1).loc[top_result_no_wbsdr.columns].sort_values(['emptiness', 'hat'], ascending=False, na_position='first')

st_result_no_websdr = top_result_no_wbsdr[sort_key.index]

fontsize = 17
fig, ax = plt.subplots()
fig.set_size_inches(40, 10)
im = ax.pcolormesh(st_result_no_websdr.values, norm='log', cmap='plasma')
ax.set_xticks(np.arange(st_result_no_websdr.columns.size), labels=st_result_no_websdr.columns, rotation=90, ha="right",
         rotation_mode="anchor")
ax.set_yticks(np.arange(years_labels.size), labels=years_labels, fontsize=fontsize)

cbar = plt.colorbar(im, ax=ax, pad=0.01)
cbar.ax.tick_params(labelsize=fontsize)
cbar.ax.set_ylabel('Percentage of traffic', fontsize=fontsize)
plt.tight_layout()
plt.show()

# Sort only by alltime referrals
hosts_all_time_contribution = top_result_no_wbsdr_dist.sum().sort_values(ascending=False)
atc_top_result = top_result_no_wbsdr_dist[hosts_all_time_contribution.index]

fig, ax = plt.subplots()
fig.set_size_inches(40, 15)
im = ax.pcolormesh(atc_top_result.values, norm='log', cmap='plasma')
ax.set_xticks(np.arange(atc_top_result.columns.size), labels=atc_top_result.columns, rotation=90, ha="right",
         rotation_mode="anchor")
ax.set_yticks(np.arange(years_labels.size), labels=years_labels)
cbar = ax.figure.colorbar(im, ax=ax)
plt.tight_layout()
plt.show()

###
# Plot the count of unique referrers each month
###
amount_of_hosts = result.count(axis=1)
amount_of_hosts.plot()
plt.show()

host_distribution_start = result_no_websdr.loc[(2014, 4)]
host_distribution_start = host_distribution_start[~host_distribution_start.isna()]

host_distribution_end = result_no_websdr.loc[(2022, 1)]
host_distribution_end = host_distribution_end[~host_distribution_end.isna()]

# As the distribution is very logarithmic, make the buckets in log space
max_referrals = int(max(host_distribution_start.max(), host_distribution_end.max()))
exponential_buckets = np.logspace(0, np.log10(max_referrals + 1000))

start_hist = host_distribution_start.plot.hist(bins=exponential_buckets)
end_hist = host_distribution_end.plot.hist(bins=exponential_buckets, alpha=0.7)
# plt.plot([0, max_referrals], [])
plt.xscale('log')
plt.yscale('log')
# plt.ylabel('density')
plt.xlabel('Referrals')
plt.legend(['April 2014', 'January 2022'])
plt.show()
