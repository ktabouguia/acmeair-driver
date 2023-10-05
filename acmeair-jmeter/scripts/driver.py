import subprocess
import os
from sdcclient import IbmAuthHelper, SdMonitorClient
import sys
import json
from collections import defaultdict
import csv

# Add the monitoring instance information that is required for authentication
URL = "https://ca-tor.monitoring.cloud.ibm.com"
APIKEY = os.environ['IBMCLOUD_API_KEY']
GUID = "b92c514a-ca21-4548-b3f0-4d6391bab407"
ibm_headers = IbmAuthHelper.get_headers(URL, APIKEY, GUID)
# Instantiate the Python client
sdclient = SdMonitorClient(sdc_url=URL, custom_headers=ibm_headers)

metrics_to_collect = [
    # JVM metrics
    "jmx_jvm_heap_used_percent",
    "jmx_jvm_gc_global_time",
    # System metrics
    "sysdig_container_cpu_used_percent",
    "sysdig_container_thread_count",
    # App metrics
    "sysdig_container_net_http_request_count",
    "sysdig_container_net_http_request_time",
]

# Write samples to a CSV file named output/<metric>.csv for metric
def write_csv(start, end, metric, samples):
    with open(f'output/{metric}.csv', 'w', newline='') as csvfile:
        fieldnames = list(samples[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for sample in samples:
            writer.writerow(sample)

# Preprocesses metric results and writes them a CSV file
# in the output folder
def write_metric_result(metric, index, res, start, end):
    samples = res['data']

    processed_samples = defaultdict(dict)
    processed_samples_with_timestamp = []

    for sample in samples:
        data = sample['d']
        processed_samples[sample['t']][data[0]] = data[index + 1]

    for timestamp, values in processed_samples.items():
        values['timestamp'] = timestamp
        processed_samples_with_timestamp.append(values)

    write_csv(start, end, metric, processed_samples_with_timestamp)

# Preprocesses Sysdig metric results and write a CSV file
# with the metric results for each metric.
def write_result(metrics_to_collect, res):
    start = res['start']
    end = res['end']
    for i, metric in enumerate(metrics_to_collect):
        write_metric_result(metric, i, res, start, end)

# Performs a JMeter load test with the given parameters
def load_test(log_file = 'output_logs.txt', thread_count = 60, duration = 600, ramp = 30, delay = 0):
    subprocess.call(
        [
            f'./apache-jmeter-5.6.2/bin/jmeter -n -t ./AcmeAir-microservices-mpJwt.jmx -DusePureIDs=true  -j logs/{log_file} -JTHREAD={thread_count} -JUSER=999 -JDURATION={duration} -JRAMP={ramp} -JDELAY=0'
        ],
        shell=True,
    )

# Pulls the list of provided metrics from Sysdig for the given time range
def get_metrics(metrics_to_collect, start = -600, end = 0):
    # Specify the ID for keys, and ID with aggregation for values
    metrics = [{"id": "kube_workload_name"}] + [{"id": metric, "aggregations": {"time": "timeAvg", "group": "avg"}} for metric in metrics_to_collect]

    # Add a data filter or set to None if you want to see "everything"
    filter = "kube_namespace_name = 'acmeair-g2'"

    # Sampling time:
    #  - for time series: sampling is equal to the "width" of each data point (expressed in seconds)
    #  - for aggregated data (similar to bar charts, pie charts, tables, etc.): sampling is equal to 0
    sampling = 60

    if (start < 0 and (start / (-sampling)) > 600):
        # Fail because we can't pull more than 600 samples at a time from Sysdig
        print(f"Last {start} divided by sampling {sampling} can not be greater than 600: Last is too big")
        return

    # Load data
    ok, res = sdclient.get_data(metrics, start, end, sampling, filter=filter)
    write_result(metrics_to_collect, res)


def main():
    # 60 threads
    # load_test(thread_count = 60, duration = 100)
    #get_metrics(start = -100, end = 0)

    # 10 threads
    # load_test(thread_count = 6, duration = 100)
    get_metrics(metrics_to_collect, start = -36000, end = 0)

if __name__ == '__main__':
    main()