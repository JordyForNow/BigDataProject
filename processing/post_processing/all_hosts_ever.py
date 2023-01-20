import pandas as pd
from os import path

#%%

# Use the unprocessed hosts here to debug the post-processing
folder = '../../Data/referrer-monthly-hosts'
out_file = '../../Data/all_hosts.csv'

years = range(2014, 2023)

months = range(1, 13)

print(path.abspath(path.curdir))

#%%
dataframes = []

for year in years:
    for month in months:
        file_path = f'{folder}/{year}/{year}-{month}.csv'
        if path.exists(file_path):
            df = pd.read_csv(file_path, header=None, names=['count', 'host'])
            dataframes.append(df)
            print(file_path, df.size)

#%%
df_concat = pd.concat(dataframes)

#%%
all_hosts = df_concat.groupby('host', dropna=False).sum().reset_index()

#%%
all_hosts.to_csv(out_file, index=False)