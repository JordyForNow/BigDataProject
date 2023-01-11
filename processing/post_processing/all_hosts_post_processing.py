import pandas as pd
from post_processing import post_process

in_path = '../../Data/all_hosts.csv'
out_path = '../../Data/all_hosts_post-processed.csv'

df = pd.read_csv(in_path)

test_df = post_process(df)

test_df.to_csv(out_path, index=False)

host = test_df['host']