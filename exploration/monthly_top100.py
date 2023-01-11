import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path

in_folder = '../Data/referrer-monthly-hosts_processed'
out_file = '../Data/referrer-monthly-top100.csv'

# Index for the places 1 through 100
top100_index = pd.Index(range(1, 101))

monthly_dataframes = []

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path)

            # Get the top 100 most occurring referrers
            top100 = df.nlargest(100, 'count').set_index(top100_index)
            # Label for which month this top 100 is
            top100['year'] = year
            top100['month'] = month

            monthly_dataframes.append(top100)

# Combine all top 100s in one table
all_months = pd.concat(monthly_dataframes)

# Make each row a place in the top 100, with months as columns
result = all_months.pivot(columns=['year', 'month'], values=['host', 'count'])
result.to_csv(out_file)
