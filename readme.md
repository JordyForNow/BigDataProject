## processing folder
The files in the main folder are used for processing the big data into small data.
The main file here is [monthly_host_country_referrals.py](processing%2Fmonthly_host_country_referrals.py),
which pre-processes referrer urls to just the host,
and then aggregates the visits for referrers per country of origin.

[hdsf_to_local.py](processing%2Fhdsf_to_local.py) uses pandas to transfer files from the HDFS to the normal filesystem on the cluster,
without losing the file structure and naming.
It does need to be run with Python3 and pandas installed to work.

### post_processing folder
This folder contains the scripts to process the small data further
by cleaning strings up further and grouping hosts.
The main logic for this can be found in [post_processing.py](processing%2Fpost_processing%2Fpost_processing.py),
with the order scripts applying its functions to different data.

For example, [all_hosts_ever.py](processing%2Fpost_processing%2Fall_hosts_ever.py) aggregates all referrers in the dataset,
with [all_hosts_post_processing.py](processing%2Fpost_processing%2Fall_hosts_post_processing.py) applying the grouping to this data.
With [mapping.py](processing%2Fpost_processing%2Fmapping.py) the difference between these datasets can be turned into a mapping
of original to grouped hostnames for easy comparison.

To apply the processing to the main data [monthly_ch_post_processing.py](post_processing%2Fmonthly_ch_post_processing.py) is used,
which keeps the country data alongside the referrers.

## exploration folder
This folder contains scripts for examining the processed small data.