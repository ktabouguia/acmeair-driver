# Reference the Python client
from sdcclient import IbmAuthHelper, SdMonitorClient
import sys
import os
import json
# Add the monitoring instance information that is required for authentication
URL = "https://ca-tor.monitoring.cloud.ibm.com"
APIKEY = os.environ['IBMCLOUD_API_KEY']
GUID = "b92c514a-ca21-4548-b3f0-4d6391bab407"
ibm_headers = IbmAuthHelper.get_headers(URL, APIKEY, GUID)

# Instantiate the Python client
sdclient = SdMonitorClient(sdc_url=URL, custom_headers=ibm_headers)

# Specify the ID for keys, and ID with aggregation for values
metrics = [
   {"id": "kube_workload_name"},
   {"id": "sysdig_container_cpu_used_percent", "aggregations": {"time": "timeAvg", "group": "avg"}}
]

# Add a data filter or set to None if you want to see "everything"
filter = "kube_namespace_name = 'acmeair-g2'"

# Time window:
#  - for "from A to B": start is equal to A, end is equal to B (expressed in seconds)
#  - for "last X seconds": start is equal to -X, end is equal to 0
start = -600
end = 0

# Sampling time:
#  - for time series: sampling is equal to the "width" of each data point (expressed in seconds)
#  - for aggregated data (similar to bar charts, pie charts, tables, etc.): sampling is equal to 0
sampling = 60

# Load data
ok, res = sdclient.get_data(metrics, start, end, sampling, filter=filter)
print((json.dumps(res, sort_keys=True, indent=4)))