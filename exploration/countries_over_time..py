import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from matplotlib import pyplot as plt

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

countries_all_time = result.sum().astype('int').sort_values(ascending=False)

all_time_visits = countries_all_time.sum()
countries_all_time_percentage = countries_all_time / all_time_visits * 100
print(countries_all_time_percentage[:10].sum())

countries_all_time_labels = countries_all_time.reset_index()['country']
countries_all_time_labels[10:] = None

pieplot = countries_all_time.plot.pie(labels=countries_all_time_labels, title='Country distribution over whole timespan')
pieplot.figure.show()
pieplot.figure.savefig('../Plots/all_time_countries.png', transparent=True)

alltime_top_countries = countries_all_time.reset_index()[:5]['country']
alltime_top_all_months = result[alltime_top_countries]

all_visits_all_months = result.sum(axis=1).astype('int')
alltime_top_all_months['Total'] = all_visits_all_months

lineplot = alltime_top_all_months.plot.line(figsize=(20, 10))
lineplot.grid(axis='y')
lineplot.figure.show()
lineplot.figure.savefig('../Plots/top_alltime_countries_nonlog.png')

lineplot = alltime_top_all_months.plot.line(logy=True, figsize=(20, 10))
lineplot.grid(axis='y')
lineplot.figure.show()
lineplot.figure.savefig('../Plots/top_alltime_countries.png')

alltime_std = all_visits_all_months.std()

# detrend by taking the difference with the previous month
derivative = all_visits_all_months[1:] - all_visits_all_months.values[:-1]
der_plot = derivative.plot()
plt.hlines(alltime_std, xmin=0, xmax=derivative.size-1, colors='C1')
plt.show()

# take the moving average over a year
der_moving_average = derivative.rolling(window=12).mean()
der_moving_std = derivative.rolling(window=12).std()

derivative.plot(figsize=(20, 10), title='Traffic Derivative')
der_moving_average.plot()
der_moving_std.plot()

plt.legend(['Traffic', 'Year average', 'Year std'])
plt.show()

# look at country distribution over time
# Sort countries by alltime visits
sorted_countries_key = countries_all_time.reset_index()['country']
result_dist = result.div(all_visits_all_months, axis=0)
sorted_result_dist = result_dist[sorted_countries_key]
sorted_result_dist.plot.bar(stacked=True, legend=False, figsize=(20, 10), title='Country Traffic Distribution', color=['20BF55'])

# handles, labels = plt.axes().get_legend_handles_labels()
plt.legend(sorted_countries_key[:30], bbox_to_anchor=(1.04, 1), loc='upper left')
plt.savefig('../Plots/country_distribution.png')
plt.show()

moving_average = all_visits_all_months.rolling(5, center=True).mean()
detrended = all_visits_all_months - moving_average
# detrended_ma = detrended.rolling(3, center=True).mean()
detrended_ma = detrended
detrended_ma.plot()
detrended_std = detrended_ma.std()
plt.hlines(0, 0, detrended_ma.size - 1, colors='black')
plt.hlines([detrended_std, -detrended_std], 0, detrended_ma.size-1, colors='C1')
plt.show()