import pandas as pd
from post_processing import post_process

path = 'all_hosts.csv'
df = pd.read_csv(path)

test_df = post_process(df)

test_df.to_csv('all_hosts_post-processed.csv', index=False)

host = test_df['host']