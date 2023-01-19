import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from os import path
from post_processing import *

in_folder = '../../Data/referrer-monthly-host-countries'
out_folder = '../../Data/referrer-monthly-host-countries_processed'

for year in range(2014, 2023):
    for month in range(1, 13):
        in_path = f'{in_folder}/{year}/{year}-{month}.csv'
        out_path = f'{out_folder}/{year}'
        out_file = f'{out_path}/{year}-{month}.csv'

        if path.exists(in_path):
            df = pd.read_csv(in_path, header=None, names=['host', 'country', 'count'])

            df = simple_process(df)
            df = host_process(df)

            df = df.groupby(['host', 'country']).sum().reset_index()

            os.makedirs(out_path, exist_ok=True)

            df.to_csv(out_file, index=False)

            print(in_path, 'to', out_file)
