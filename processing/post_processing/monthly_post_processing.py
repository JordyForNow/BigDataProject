import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from post_processing import post_process

in_folder = '../../Data/referrer-monthly-hosts'
out_folder = '../../Data/referrer-monthly-hosts_processed'

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'
        out_path = f'{out_folder}/{year}'
        out_file = f'{out_path}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path, header=None, names=['count', 'host'])

            df = post_process(df)

            df = df.groupby('host').sum().reset_index()

            os.makedirs(out_path, exist_ok=True)

            df.to_csv(out_file, index=False)

            print(in_path, 'to', out_file)
