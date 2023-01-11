import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path

in_folder = '../referrer-monthly-hosts_processed'
out_file = 'referrer-monthly-top100.csv'

top100_index = pd.Index(range(1, 101))

monthly_dataframes = []

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path)

            top100 = df.nlargest(100, 'count').set_index(top100_index)
            top100['year'] = year
            top100['month'] = month

            monthly_dataframes.append(top100)

all_months = pd.concat(monthly_dataframes)

result = all_months.pivot(columns=['year', 'month'], values=['host', 'count'])
result.to_csv(out_file)
