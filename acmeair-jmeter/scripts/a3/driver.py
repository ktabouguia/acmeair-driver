import subprocess

run_parameters = {
    # "TEST_HIGH_LOAD": {"thread_count": 600, "duration": 900, "ramp": 25, "delay": 0},
    "TEST_MEDIUM_LOAD": {"thread_count": 300, "duration": 900, "ramp": 25, "delay": 0},
    "TEST_LOW_LOAD": {"thread_count": 150, "duration": 900, "ramp": 25, "delay": 0},
}

# Performs a JMeter load test with the given parameters
def load_test(log_file = 'output_logs.txt', thread_count = 60, duration = 600, ramp = 30, delay = 0):
    subprocess.call(
        [
            f'./apache-jmeter-5.6.2/bin/jmeter -n -t ./AcmeAir-microservices-mpJwt.jmx -DusePureIDs=true  -j logs/{log_file} -JTHREAD={thread_count} -JUSER=999 -JDURATION={duration} -JRAMP={ramp} -JDELAY=0'
        ],
        shell=True,
    )

def main():
    for name, parameters in run_parameters.items():
        thread_count = parameters['thread_count']
        duration = parameters['duration']
        ramp = parameters['ramp']
        delay = parameters['delay']
        load_test(thread_count = thread_count, duration = duration, ramp = ramp, delay = delay)

if __name__ == '__main__':
    main()