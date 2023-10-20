import csv
import os
import subprocess
from collections import defaultdict
from sdcclient import IbmAuthHelper, SdMonitorClient

# Add the monitoring instance information that is required for authentication
URL = "https://ca-tor.monitoring.cloud.ibm.com"
APIKEY = os.environ['IBMCLOUD_API_KEY']
GUID = "b92c514a-ca21-4548-b3f0-4d6391bab407"
ibm_headers = IbmAuthHelper.get_headers(URL, APIKEY, GUID)
# Instantiate the Python client
sdclient = SdMonitorClient(sdc_url=URL, custom_headers=ibm_headers)


standard_metrics = {
    "sysdig_container_net_http_request_time" : {"group": "avg"},
}
status_code_metrics = {
    "sysdig_container_net_http_statuscode_request_count": {"group": "avg"},
}
metrics_to_collect = {
    "kube_namespace_name = 'acmeair-g2'": standard_metrics,
    "kube_namespace_name = 'acmeair-g2' and net_http_statuscode in ('503', '502', '500', '400', '401', '403')": status_code_metrics,
}

run_parameters = {
    # "TEST_HIGH_LOAD": { "thread_count": 600, "duration": 900, "ramp": 25, "delay": 0 },
    # "TEST_MEDIUM_LOAD": { "thread_count": 300, "duration": 900, "ramp": 25, "delay": 0},
    "TEST_LOW_LOAD": { "thread_count": 150, "duration": 900, "ramp": 25, "delay": 0}
}

default_csv_header = ['acmeair-bookingservice','acmeair-customerservice','acmeair-flightservice','acmeair-authservice','acmeair-mainservice','timestamp']

# Write samples to a CSV file named output/<metric>.csv for metric
def write_csv(test_name, metric, samples):
    with open(f'output/configuration1/{metric}_{test_name}_.csv', 'w', newline='') as csvfile:
        if (len(samples) == 0):
            fieldnames = default_csv_header
        else:
            fieldnames = list(samples[0].keys())

        print(fieldnames)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for sample in samples:
            writer.writerow(sample)

# Preprocesses metric results and writes them a CSV file
# in the output folder
def write_metric_result(test_name, metric, index, res):
    samples = res['data']

    processed_samples = defaultdict(dict)
    processed_samples_with_timestamp = []

    for sample in samples:
        data = sample['d']
        processed_samples[sample['t']][data[0]] = data[index + 1]

    for timestamp, values in processed_samples.items():
        values['timestamp'] = timestamp
        processed_samples_with_timestamp.append(values)

    write_csv(test_name, metric, processed_samples_with_timestamp)

# Preprocesses Sysdig metric results and write a CSV file
# with the metric results for each metric.
def write_result(test_name, metrics_to_collect, res):
    for i, metric in enumerate(metrics_to_collect):
        write_metric_result(test_name, metric, i, res)

# Performs a JMeter load test with the given parameters
def load_test(log_file = 'output_logs.txt', thread_count = 60, duration = 600, ramp = 30, delay = 0):
    subprocess.call(
        [
            f'./apache-jmeter-5.6.2/bin/jmeter -n -t ./AcmeAir-microservices-mpJwt.jmx -DusePureIDs=true  -j logs/{log_file} -JTHREAD={thread_count} -JUSER=999 -JDURATION={duration} -JRAMP={ramp} -JDELAY=0'
        ],
        shell=True,
    )

# Pulls the list of provided metrics from Sysdig for the given time range
def get_metrics(test_name, metrics_to_collect, filter, start = -600, end = 0):
    # Specify the ID for keys, and ID with aggregation for values
    metrics = [{"id": "kube_workload_name"}] + [{"id": metric, "aggregations": aggregations} for metric, aggregations in metrics_to_collect.items()]

    # Sampling time:
    #  - for time series: sampling is equal to the "width" of each data point (expressed in seconds)
    #  - for aggregated data (similar to bar charts, pie charts, tables, etc.): sampling is equal to 0
    sampling = 10

    if (start < 0 and (start / (-sampling)) > 600):
        # Fail because we can't pull more than 600 samples at a time from Sysdig
        print(f"Last {start} divided by sampling {sampling} can not be greater than 600: Last is too big")
        return

    # Load data
    ok, res = sdclient.get_data(metrics, start, end, sampling, filter=filter)

    if ok:
        write_result(test_name, metrics_to_collect, res)
    else:
        print(f"Failed to pull metrics: {res}")

def get_all_metrics(name, start, end):
    for filter, metrics_group in metrics_to_collect.items():
        get_metrics(name, metrics_group, filter, start, end)

def main():
    for name, parameters in run_parameters.items():
        thread_count = parameters['thread_count']
        duration = parameters['duration']
        ramp = parameters['ramp']
        delay = parameters['delay']
        load_test(thread_count = thread_count, duration = duration, ramp = ramp, delay = delay)
        get_all_metrics(name, start = -duration, end = 0)

if __name__ == '__main__':
    main()