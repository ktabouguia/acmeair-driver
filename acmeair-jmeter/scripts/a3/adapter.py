import csv
import os
import statistics
import time
import openshift as oc
from collections import defaultdict
from sdcclient import IbmAuthHelper, SdMonitorClient

# Add the monitoring instance information that is required for authentication
URL = "https://ca-tor.monitoring.cloud.ibm.com"
APIKEY = os.environ['IBMCLOUD_API_KEY']
GUID = "b92c514a-ca21-4548-b3f0-4d6391bab407"
ibm_headers = IbmAuthHelper.get_headers(URL, APIKEY, GUID)
# Instantiate the Python client
sdclient = SdMonitorClient(sdc_url=URL, custom_headers=ibm_headers)

metric_types = {
    'latency': 'sysdig_container_net_http_request_time',
    'error_rate': 'sysdig_container_net_http_statuscode_request_count',
    "cpu_used": "sysdig_container_cpu_used_percent",
    "memory_used": "sysdig_container_memory_used_percent",
}
standard_metrics = {
    "sysdig_container_net_http_request_time" : {"group": "avg"},
    "sysdig_container_cpu_used_percent" : {"group": "avg"},
    "sysdig_container_memory_used_percent" : {"group": "avg"},
}
status_code_metrics = {
    "sysdig_container_net_http_statuscode_request_count": {"group": "avg"},
}
metrics_to_collect = {
    "kube_namespace_name = 'acmeair-g2'": standard_metrics,
    "kube_namespace_name = 'acmeair-g2' and net_http_statuscode in ('503', '502', '500', '400', '401', '403')": status_code_metrics,
}

configurations = {
    'c1': { 'cpu': '250m', 'memory': '250Mi', 'pod_count': 1},
    'c2': { 'cpu': '250m', 'memory': '500Mi', 'pod_count': 1},
    'c3': { 'cpu': '250m', 'memory': '500Mi', 'pod_count': 2},
}

configuration_maps = {
    '250mx250Mix1': 'c1',
    '250mx500Mix1': 'c2',
    '250mx500Mix2': 'c3',
}

service_list = ['acmeair-bookingservice','acmeair-customerservice','acmeair-flightservice','acmeair-authservice','acmeair-mainservice']

# Preprocesses metric results and writes them a CSV file
# in the output folder
def group_metric_result(index, res):
    samples = res['data']

    processed_samples = defaultdict(list)

    for sample in samples:
        data = sample['d']
        processed_samples[data[0]].append(data[index + 1])

    return dict(processed_samples)

# Preprocesses Sysdig metric results and write a CSV file
# with the metric results for each metric.
def group_metrics_by_service(metrics_to_collect, res):
    by_service = defaultdict()
    for i, metric in enumerate(metrics_to_collect):
        by_service[metric] = group_metric_result(i, res)

    return dict(by_service)

# Pulls the list of provided metrics from Sysdig for the given time range
def get_metrics(metrics_group, filter, start = -600, end = 0):
    # Specify the ID for keys, and ID with aggregation for values
    metrics = [{"id": "kube_workload_name"}] + [{"id": metric, "aggregations": aggregations} for metric, aggregations in metrics_group.items()]

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
        return group_metrics_by_service(metrics_group, res)
    else:
        print(f"Failed to pull metrics: {res}")

def get_all_metrics(start, end):
    res = {}
    for filter, metrics_group in metrics_to_collect.items():
        res = res | get_metrics(metrics_group, filter, start, end)

    return res

def compute_mean_by_service(metrics, dim, services):
    values = defaultdict(list) | metrics[metric_types[dim]]
    mean_by_service = {}
    for service in services:
        mean = 0
        if (len(values[service]) == 0):
            mean = 0
        else:
            mean = statistics.mean(values[service])
        mean_by_service[service] = mean

    return mean_by_service

latency_weight = 0.65
error_rate_weight = 0.35

def latency_to_preference(latency):
    if latency < 1000:
        return 1
    if 1000 <= latency < 3000:
        return 0.5
    if 3000 <= latency < 5000:
        return 0.2
    if latency > 5000:
        return 0

def rate_error_count_to_preference(rate_error_count):
    if rate_error_count < 1:
        return 1
    if 1 <= rate_error_count < 3:
        return 0.5
    if rate_error_count > 3:
        return 0

def compute_utility_function_by_service(means_by_services):
    utility_by_service = {}
    for service, values in means_by_services.items():
        wP, pP = latency_weight, latency_to_preference(values['latency'])
        wE, pE = error_rate_weight, rate_error_count_to_preference(values['error_rate'])
        utility = (wP * pP) + (wE * pE)
        utility_by_service[service] = utility

    return utility_by_service

def find_next_configuration(cur_cpu, cur_memory, cur_pod_count, down_scale = False):
    config_key = f'{cur_cpu}x{cur_memory}x{cur_pod_count}'
    if config_key in configuration_maps:
        current_config = configuration_maps[f'{cur_cpu}x{cur_memory}x{cur_pod_count}']
    else:
        current_config = None

    next_configuration = None
    if down_scale == False:
        if current_config == 'c1':
            next_configuration = configurations['c2']
        elif current_config == 'c2':
            next_configuration = configurations['c3']
        else:
            next_configuration = { 'cpu': '250m' , 'memory': '500Mi' , 'pod_count': cur_pod_count*2 }

        if next_configuration and  next_configuration != current_config:
            print('Upscaling....')
    else:
        if current_config == 'c1':
            next_configuration = None
        elif current_config == 'c2':
            next_configuration = configurations['c1']
        elif current_config == 'c3':
            next_configuration = configurations['c2']
        else:
            next_configuration = { 'cpu': '250m' , 'memory': '500Mi' , 'pod_count': cur_pod_count/2 }

        if next_configuration and next_configuration != current_config:
            print('Downscaling....')

    return next_configuration


def plan(service, down_scale = False):
    obj = oc.selector(f'deployment.apps/{service}').object()
    limits = obj.model['spec']['template']['spec']['containers'][0]['resources']['limits']
    current_cpu, current_memory = limits['cpu'], limits['memory']
    current_pod_count = obj.model['spec']['replicas']

    # print(f'Current configuration for service {service}')
    # print(f'current cpu: {current_cpu}')
    # print(f'current memory: {current_memory}')
    # print(f'current pod count: {current_pod_count}')

    return find_next_configuration(current_cpu, current_memory, current_pod_count, down_scale)

def apply_resources(obj, plan):
    obj.model['spec']['template']['spec']['containers'][0]['resources']['requests']['memory'] = plan["memory"]
    obj.model['spec']['template']['spec']['containers'][0]['resources']['limits']['memory'] = plan["memory"]
    obj.model['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = plan["cpu"]
    obj.model['spec']['template']['spec']['containers'][0]['resources']['limits']['cpu'] = plan["cpu"]
    obj.model['spec']['replicas'] = plan["pod_count"]
    obj.apply()

def execute(service, execution_plan):
    obj = oc.selector(f'deployment.apps/{service}').object()

    print(f"Executing adaption for service {service} - new configuration: {execution_plan}")

    apply_resources(obj, execution_plan)

    print("Adaptation completed")

    return


def initialize_services(service_list, configuration):
    print("Initializing resources for all services")
    for service in service_list:
        execute(service, configuration)

def adapt(start, end):
    metrics_grouped_by_service = get_all_metrics(start, end)

    # Get services
    services = []
    for _, metrics in metrics_grouped_by_service.items():
        for service, _ in metrics.items():
            if service not in services and service in service_list:
                services.append(service)

    means_by_service = defaultdict(dict)

    # Compute latency mean by service
    latency_mean_by_service = compute_mean_by_service(metrics_grouped_by_service, 'latency', services)
    for service in services:
        means_by_service[service]['latency'] = (latency_mean_by_service[service] / (10 ** 6))

    # Compute error rate mean by service
    error_rate_mean_by_service = compute_mean_by_service(metrics_grouped_by_service, 'error_rate', services)
    for service in services:
        means_by_service[service]['error_rate'] = error_rate_mean_by_service[service]

    cpu_used_mean_by_service = compute_mean_by_service(metrics_grouped_by_service, 'cpu_used', services)
    for service in services:
        means_by_service[service]['cpu_used'] = error_rate_mean_by_service[service]

    should_wait = False
    # Compute utility function by service
    utilities_by_service = compute_utility_function_by_service(means_by_service)
    for service in services:
        print(f"\n\nAttempting to adapt service {service}")
        utility = utilities_by_service[service]
        print(f'\nUtiliy value: {utility}')

        if utility == 1 and cpu_used_mean_by_service[service] < 5:
            execution_plan = plan(service, down_scale=True)
            if execution_plan:
                execute(service, execution_plan)
                should_wait = True
            continue
        elif utility < 0.7:
            execution_plan = plan(service)
            if execution_plan:
                execute(service, execution_plan)
                should_wait = True
            continue
        else:
            continue

    return should_wait

def main():
    initialize_services(service_list, configurations['c1'])
    time.sleep(180)
    while True:
        print("Starting adaptation loop")
        should_wait = adapt(start = -60, end = 0)
        if should_wait == True:
            time.sleep(360)
        else:
            time.sleep(10)

if __name__ == '__main__':
    main()