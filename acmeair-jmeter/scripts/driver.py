import subprocess
import os
from sdcclient import IbmAuthHelper, SdMonitorClient
import sys
import json

# Add the monitoring instance information that is required for authentication
URL = "https://ca-tor.monitoring.cloud.ibm.com"
APIKEY = os.environ['IBMCLOUD_API_KEY']
GUID = "b92c514a-ca21-4548-b3f0-4d6391bab407"

ibm_headers = IbmAuthHelper.get_headers(URL, APIKEY, GUID)

# Instantiate the Python client
sdclient = SdMonitorClient(sdc_url=URL, custom_headers=ibm_headers)

def load_test(log_file = 'output_logs.txt', thread_count = 60, duration = 600, ramp = 30, delay = 0):
    subprocess.call(
        [
            f'./apache-jmeter-5.6.2/bin/jmeter -n -t ./AcmeAir-microservices-mpJwt.jmx -DusePureIDs=true  -j logs/{log_file} -JTHREAD={thread_count} -JUSER=999 -JDURATION={duration} -JRAMP={ramp} -JDELAY=0'
        ],
        shell=True,
    )

def get_metrics(start = -600, end = 0):
    # Specify the ID for keys, and ID with aggregation for values
    metrics = [
       {"id": "kube_workload_name"},
       {"id": "sysdig_container_cpu_used_percent", "aggregations": {"time": "timeAvg", "group": "avg"}}
    ]

    # Add a data filter or set to None if you want to see "everything"
    filter = "kube_namespace_name = 'acmeair-g2'"

    # Sampling time:
    #  - for time series: sampling is equal to the "width" of each data point (expressed in seconds)
    #  - for aggregated data (similar to bar charts, pie charts, tables, etc.): sampling is equal to 0
    sampling = 60

    # Load data
    ok, res = sdclient.get_data(metrics, start, end, sampling, filter=filter)
    print((json.dumps(res, sort_keys=True, indent=4)))

def main():
    # 60 threads
    load_test(thread_count = 60, duration = 100)
    get_metrics(start = -100, end = 0)

    # 10 threads
    load_test(thread_count = 6, duration = 100)
    get_metrics(start = -100, end = 0)

if __name__ == '__main__':
   main()