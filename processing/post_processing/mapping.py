import pandas as pd

from post_processing import simple_process, host_process

in_path = '../../Data/all_hosts.csv'
out_path = '../../Data/post-processing_mapping.csv'

simple_processed_df = simple_process(pd.read_csv(in_path))
host_processed_df = host_process(simple_process(pd.read_csv(in_path)))

# get the difference between the two
changed = host_processed_df['host'] != simple_processed_df['host']

original_hosts = simple_processed_df[changed]['host']
new_hosts = host_processed_df[changed]['host']

mapping_df = pd.DataFrame({'original_host': original_hosts, 'host': new_hosts}).drop_duplicates()

mapping_df.to_csv(out_path, index=False)
