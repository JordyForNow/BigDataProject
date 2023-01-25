import pandas as pd
from post_processing import post_process

in_path = '/Users/fabin/Downloads/TopCountries_WarSpike.csv'

out_path = '/Users/fabin/Downloads/TopCountries_WarSpike1.csv'

df = pd.read_csv(in_path)
df = df.iloc[: , 1:]
df = df.drop(columns=['country_name'])
df1 = pd.read_csv(in_path)

print(df1.head())
df2 = df1.drop(columns=['host'])
print(df2.head())

test_df = post_process(df)
print(test_df)
# test_df.to_csv(out_path, index=False)

host = test_df['host']
df2['host'] = host
print(df2)

df2.to_csv(out_path, index=False)