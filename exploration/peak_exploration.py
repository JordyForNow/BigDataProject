import math
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from matplotlib import pyplot as plt
import matplotlib as mpl
import numpy as np

in_folder = '../Data/referrer-monthly-host-countries_processed'

monthly_dataframes = []

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path)

            country_count_df = df[['country', 'host', 'count']].groupby(['country', 'host']).sum().reset_index()
            country_count_df['year'] = year
            country_count_df['month'] = month

            monthly_dataframes.append(country_count_df)

all_months = pd.concat(monthly_dataframes)

result = all_months.pivot(index=['year', 'month'], values='count', columns=['country', 'host'])
result_no_websdr = result.drop(columns='websdr.ewi.utwente.nl', level=1)

visual_lh_countries = ['RE', 'MZ', 'NC', 'CK', 'PF']

country = 'CA'
country_res = result[country]

start_month = 12*(2017-2014)
end_month = 12*(2018-2014)

months_of_interest = country_res.iloc[start_month:end_month]
hosts_of_interest = months_of_interest.loc[:,(~months_of_interest.isna()).any()]

host_totals = hosts_of_interest.sum().sort_values(ascending=False)
all_visits = host_totals.sum()

host_distribution = host_totals / all_visits * 100


# fig, ax = plt.subplots()
# fig.set_size_inches(40, 15)
# im = ax.pcolormesh(top_result_no_wbsdr.values, norm='log', cmap='plasma')
# ax.set_xticks(np.arange(top_result_no_wbsdr.columns.size), labels=top_result_no_wbsdr.columns, rotation=90, ha="right",
#          rotation_mode="anchor")
# ax.set_yticks(np.arange(years_labels.size), labels=years_labels)
# cbar = ax.figure.colorbar(im, ax=ax)
# plt.tight_layout()
# plt.show()

country_result = all_months.groupby(['country', 'year', 'month']).sum().reset_index().pivot(index=['year', 'month'], values='count', columns='country')

countries_all_time = country_result.sum().sort_values(ascending=False)

countries_sort_key = countries_all_time.index

all_traffic = countries_all_time.sum()
countries_all_time_percentage = countries_all_time / all_traffic * 100

months_of_interest_indexes = [
    12 * (2017 - 2014) + 7 - 1,
    12 * (2017 - 2014) + 12 - 1,
    12 * (2019 - 2014) + 2 - 1,
    12 * (2021 - 2014) + 2 - 1
]

countries_all_time_percentage.name = 'All time'
all_windows = []

for month_index in months_of_interest_indexes:
    month_of_interest = country_result.iloc[month_index:month_index+1][countries_sort_key]
    window_traffic = month_of_interest.sum(axis=0)
    all_window_traffic = window_traffic.sum()
    window_traffic_percentage = window_traffic / all_window_traffic * 100
    window_traffic_percentage.name = country_result.index[month_index]
    all_windows.append(window_traffic_percentage)

traffic_percentages = pd.concat(all_windows, axis=1)

topn = 15
bar_width = 0.8

traffic_percentages[:topn].plot.bar(figsize=(8,4), width=bar_width)
countries_all_time_percentage[:topn].plot.bar(alpha=0.5, color='gray', width=bar_width)
# window_traffic_percentage[:25].plot.bar(alpha=0.7, color='C1')
# plt.legend(['All time', 'July 2017'])
plt.ylabel('Percentage of traffic')
# plt.yscale('log')
# plt.yticks([0, 10, 50], ['0', '10', '50'])
lgnd = plt.legend()
lgnd.set_title('Year, Month')
plt.tight_layout()
plt.show()

specific_country_res = result_no_websdr['FI']
specific_country_alltime = specific_country_res.sum()
specific_country_all_traffic = specific_country_alltime.sum()
specific_country_alltime_distribution = specific_country_alltime / specific_country_all_traffic * 100


moi_index = months_of_interest_indexes[5]
specific_country_month = specific_country_res.iloc[moi_index]
specific_country_month_all = specific_country_month.sum()
specific_country_month_distribution = specific_country_month / specific_country_month_all * 100
specific_country_month_distribution.name = specific_country_res.index[moi_index]


##
# Continuous dark band
##

months_of_interest_indexes = np.arange(12 * (2020 - 2014) + 6 - 1, 12 * (2020 - 2014) + 12 - 1)

countries_all_time_percentage.name = 'All time'
all_windows = []

for month_index in months_of_interest_indexes:
    month_of_interest = country_result.iloc[month_index:month_index+1][countries_sort_key]
    window_traffic = month_of_interest.sum(axis=0)
    all_window_traffic = window_traffic.sum()
    window_traffic_percentage = window_traffic / all_window_traffic * 100
    window_traffic_percentage.name = country_result.index[month_index]
    all_windows.append(window_traffic_percentage)

traffic_percentages = pd.concat(all_windows, axis=1)

topn = 15
bar_width = 0.8

traffic_percentages[:topn].plot.bar(figsize=(8,4), width=bar_width)
countries_all_time_percentage[:topn].plot.bar(alpha=0.5, color='gray', width=bar_width)
# window_traffic_percentage[:25].plot.bar(alpha=0.7, color='C1')
# plt.legend(['All time', 'July 2017'])
plt.ylabel('Percentage of traffic')
# plt.yscale('log')
# plt.yticks([0, 10, 50], ['0', '10', '50'])
lgnd = plt.legend()
lgnd.set_title('Year, Month')
plt.tight_layout()
plt.show()