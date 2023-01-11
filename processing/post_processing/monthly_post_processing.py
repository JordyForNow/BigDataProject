import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from post_processing import post_process

in_folder = '../referrer-monthly-hosts'
out_folder = '../referrer-monthly-hosts_processed'

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'
        out_path = f'{out_folder}/{year}'
        out_file = f'{out_path}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path, header=None, names=['count', 'host'])

            # lowercase
            df['host'] = df['host'].str.lower()
            # squash urls ending in dots
            df['host'] = df['host'].str.removesuffix('.')
            # squash www. duplicates
            df['host'] = df['host'].str.removeprefix('www.')

            df = post_process(df)

            df = df.groupby('host').sum().reset_index()

            os.makedirs(out_path, exist_ok=True)

            df.to_csv(out_file, index=False)

            print(in_path, 'to', out_file)
