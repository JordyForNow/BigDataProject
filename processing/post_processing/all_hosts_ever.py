import pandas as pd
from os import path

#%%

folder = '../referrer-monthly-hosts'

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

#%% preprocessing
df_concat['host'] = df_concat['host'].str.lower()
#%% squash urls ending in dots
df_concat['host'] = df_concat['host'].str.removesuffix('.')

#%% squash www. duplicates
df_concat['host'] = df_concat['host'].str.removeprefix('www.')

#%%
all_hosts = df_concat.groupby('host').sum().reset_index()

#%%
all_hosts.to_csv('all_hosts.csv', index=False)