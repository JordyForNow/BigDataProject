import math
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from matplotlib import pyplot as plt
import matplotlib as mpl
import numpy as np

in_folder = '../Data/referrer-monthly-host-countries_processed'
out_file = '../Data/all_months_countries.csv'

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

result = all_months.pivot(index=['year', 'month'], values='count', columns='host')

result_no_websdr = result.drop('websdr.ewi.utwente.nl', axis=1)

hosts_all_time = result_no_websdr.sum().astype('int').sort_values(ascending=False)

sorted_result_no_websdr = result_no_websdr[hosts_all_time.index]

all_time_visits = hosts_all_time.sum()
hosts_all_time_percentage = hosts_all_time / all_time_visits * 100

topn = 250

print(hosts_all_time_percentage[:topn].sum())

top_result_no_wbsdr = sorted_result_no_websdr.iloc[:, :topn]

rest_result = sorted_result_no_websdr.iloc[:, topn:].sum(axis=1)
top_result_no_wbsdr['REST'] = rest_result

all_visits_per_month = top_result_no_wbsdr.sum(axis=1)

top_result_no_wbsdr_dist = top_result_no_wbsdr.div(all_visits_per_month, axis=0)

# hosts_all_time_labels = hosts_all_time.reset_index()['host']
# top_hosts_all_time_labels = hosts_all_time.reset_index()['host']
# top_hosts_all_time_labels[20:] = None
#
# pieplot = hosts_all_time.plot.pie(labels=top_hosts_all_time_labels)
# plt.show()

years_labels = result.index.to_series().apply(lambda x : f"{x[0]}" if x[1] == 1 else None)

fig, ax = plt.subplots()
fig.set_size_inches(40, 15)
im = ax.pcolormesh(top_result_no_wbsdr_dist.values, norm='log', cmap='plasma')
ax.set_xticks(np.arange(top_result_no_wbsdr_dist.columns.size), labels=top_result_no_wbsdr_dist.columns, rotation=90, ha="right",
         rotation_mode="anchor")
ax.set_yticks(np.arange(years_labels.size), labels=years_labels)
cbar = ax.figure.colorbar(im, ax=ax)
plt.tight_layout()
plt.show()

emptiness = top_result_no_wbsdr_dist.count().sort_values(ascending=False)
es_top_results = top_result_no_wbsdr_dist[emptiness.index]

fig, ax = plt.subplots()
fig.set_size_inches(40, 15)
im = ax.pcolormesh(es_top_results.values, norm='log', cmap='plasma')
ax.set_xticks(np.arange(es_top_results.columns.size), labels=es_top_results.columns, rotation=90, ha="right",
         rotation_mode="anchor")
ax.set_yticks(np.arange(years_labels.size), labels=years_labels)
cbar = ax.figure.colorbar(im, ax=ax)
plt.tight_layout()
plt.show()


fig, ax = plt.subplots()
fig.set_size_inches(40, 15)
im = ax.pcolormesh(top_result_no_wbsdr.values, norm='log', cmap='plasma')
ax.set_xticks(np.arange(top_result_no_wbsdr.columns.size), labels=top_result_no_wbsdr.columns, rotation=90, ha="right",
         rotation_mode="anchor")
ax.set_yticks(np.arange(years_labels.size), labels=years_labels)
cbar = ax.figure.colorbar(im, ax=ax)
plt.tight_layout()
plt.show()

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

amount_of_hosts = result.count(axis=1)
amount_of_hosts.plot()
plt.show()

amount_of_hosts_spike = result.loc[(2019, 7)]
amount_of_hosts_spike = amount_of_hosts_spike[~amount_of_hosts_spike.isna()]

host_distribution_start = result_no_websdr.loc[(2014, 1)]
host_distribution_start = host_distribution_start[~host_distribution_start.isna()]

host_distribution_end = result_no_websdr.loc[(2022, 1)]
host_distribution_end = host_distribution_end[~host_distribution_end.isna()]

max_referrals = int(max(host_distribution_start.max(), host_distribution_end.max()))

exponential_buckets = np.logspace(0, np.log10(max_referrals + 1000))

start_hist = host_distribution_start.plot.hist(bins=exponential_buckets, density=True)
end_hist = host_distribution_end.plot.hist(bins=exponential_buckets, density=True, alpha=0.7)
# plt.plot([0, max_referrals], [])
plt.xscale('log')
plt.yscale('log')
plt.ylabel('density')
plt.legend(['januari 2014', 'januari 2022'])
plt.show()