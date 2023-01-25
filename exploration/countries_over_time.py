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

            country_count_df = df[['country', 'count']].groupby('country').sum().reset_index()
            country_count_df['year'] = year
            country_count_df['month'] = month

            monthly_dataframes.append(country_count_df)

all_months = pd.concat(monthly_dataframes)

result = all_months.pivot(index=['year', 'month'], values='count', columns='country')
# years_index = result.index.to_series().apply(lambda x : f'{x[0]}' if x[1] == 1 else None)
years_index = result.index.to_frame()
years_ticks = years_index[years_index['month'] == 1]

countries_all_time = result.sum().astype('int').sort_values(ascending=False)

all_time_visits = countries_all_time.sum()
countries_all_time_percentage = countries_all_time / all_time_visits * 100
print(countries_all_time_percentage[:10].sum())

countries_all_time_labels = countries_all_time.reset_index()['country']
countries_all_time_labels[10:] = None

pieplot = countries_all_time.plot.pie(labels=countries_all_time_labels, title='Country distribution over whole timespan')
pieplot.figure.show()
# pieplot.figure.savefig('../Plots/all_time_countries.png', transparent=True)

alltime_top_countries = countries_all_time.reset_index()[:5]['country']
alltime_top_all_months = result[alltime_top_countries]

all_visits_all_months = result.sum(axis=1).astype('int')
alltime_top_all_months['Total'] = all_visits_all_months

lineplot = alltime_top_all_months.plot.line(figsize=(20, 10))
lineplot.grid(axis='y')
lineplot.figure.show()
# lineplot.figure.savefig('../Plots/top_alltime_countries_nonlog.png')

lineplot = alltime_top_all_months.plot.line(logy=True, figsize=(20, 10))
lineplot.grid(axis='y')
lineplot.figure.show()
# lineplot.figure.savefig('../Plots/top_alltime_countries.png')

alltime_std = all_visits_all_months.std()

# detrend by taking the difference with the previous month
differencing = all_visits_all_months[1:] - all_visits_all_months.values[:-1]
dif_plot = differencing.plot()
plt.hlines(alltime_std, xmin=0, xmax=differencing.size - 1, colors='C1')
plt.show()

# take the moving average over a year
dif_moving_average = differencing.rolling(window=12).mean()
dif_moving_std = differencing.rolling(window=12).std()

differencing.plot(figsize=(6, 4), title='Traffic detrended')

# dif_moving_average.plot()
# dif_moving_std.plot()
plt.hlines([differencing.std(), -differencing.std()], 0, differencing.size - 1, colors='C1')
# plt.hlines(0, 0, differencing.size - 1, colors='black')
plt.grid(visible=True)

plt.xticks([12*i-1 for i in range(0, 10)], rotation=90, labels=np.arange(2014,2024))
plt.xlabel('Year')
plt.ylabel('Traffic Delta')

plt.tight_layout()

plt.legend(['Traffic', 'Std'])
plt.show()

# look at country distribution over time
# Sort countries by alltime visits
sorted_countries_key = countries_all_time.reset_index()['country']
result_dist = result.div(all_visits_all_months, axis=0)
sorted_result_dist = result_dist[sorted_countries_key]
# sorted_result_dist.plot.bar(stacked=True, legend=False, figsize=(20, 10), title='Country Traffic Distribution')
#
# # handles, labels = plt.axes().get_legend_handles_labels()
# plt.legend(sorted_countries_key[:30], bbox_to_anchor=(1.04, 1), loc='upper left')
# # plt.savefig('../Plots/country_distribution.png')
# plt.show()

plt.axes().xaxis.set_minor_locator(mpl.ticker.AutoMinorLocator(1))
plt.minorticks_on()

moving_average = all_visits_all_months.rolling(5, center=True).mean()
detrended = all_visits_all_months - moving_average
# detrended_ma = detrended.rolling(3, center=True).mean()
detrended_ma = detrended
detrended_ma.plot(xticks=[12*i for i in range(0, 10)], figsize=(20, 10))
detrended_std = detrended_ma.std()
plt.hlines(0, 0, detrended_ma.size - 1, colors='black')
plt.hlines([detrended_std, -detrended_std], 0, detrended_ma.size-1, colors='C1')
plt.grid(axis='x', which='both')
plt.show()


def label_filter(row):
    year = row['year']
    if not math.isnan(year):
        return f"{year:.0f}"
    else:
        return ""


labels = years_index.where(years_index['month'] == 1).apply(label_filter, result_type='reduce', axis=1)

fontsize = 17
fig, ax = plt.subplots()
fig.set_size_inches(40, 10)
# im = ax.imshow(sorted_result_dist.values)
im = ax.pcolormesh(sorted_result_dist.values, norm='log', cmap='plasma')
ax.set_xticks(np.arange(sorted_result_dist.columns.size), labels=sorted_result_dist.columns, rotation=90, ha="right",
         rotation_mode="anchor")
ax.set_yticks(np.arange(labels.size), labels=labels, fontsize=fontsize)

# cbaxes = fig.add_axes([])

cbar = plt.colorbar(im, ax=ax, pad=0.01)
cbar.ax.tick_params(labelsize=fontsize)
cbar.ax.set_ylabel('Percentage of traffic', fontsize=fontsize)
plt.tight_layout()
plt.show()

# Plot sudden spike countries
visual_lh_countries = ['RE', 'MZ', 'NC', 'CK', 'PF']
visual_lh_result = result[visual_lh_countries]



plot = visual_lh_result.plot(alpha=0.7)
plot.hlines(visual_lh_result.median(), 0, visual_lh_result.index.size - 1, linestyles='dashed', colors=['C0', 'C1', 'C2', 'C3', 'C4'])
# plt.hlines(1e4, 0, visual_lh_result.index.size - 1, linestyles='dashed', colors='black')
plot.axes.set_xticks(np.arange(9)*12, labels=np.arange(2014,2023))
plot.axes.set_xlabel('year')
plt.yscale('log')
plt.show()