import os
import subprocess
import time
import random
import csv
import paramiko
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

# (1) ############################################## Different Cross-node delays generation #############################################################

def generate_delay_matrix(num_nodes, base_latency, max_additional_latency):
    delay_matrix = [[0 for _ in range(num_nodes)] for _ in range(num_nodes)]
    for i in range(num_nodes):
        for j in range(num_nodes):
            if i != j:
                additional_latency = random.uniform(0, max_additional_latency)
                distance_factor = abs(i - j) / num_nodes
                simulated_latency = base_latency + additional_latency * distance_factor
                congestion_factor = random.uniform(0.5, 1.5)
                delay_matrix[i][j] = int(simulated_latency * congestion_factor)
    return delay_matrix

def apply_latency_between_nodes(source_node_name, username, key_path, interface, delay_matrix, node_details):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()
    try:
        source_node_ip = node_details[source_node_name]['ip']
        client.connect(source_node_ip, username=username, key_filename=key_path)
        client.exec_command(f"sudo tc qdisc del dev {interface} root")  # Clear existing rules
        client.exec_command(f"sudo tc qdisc add dev {interface} root handle 1: htb default 1")
        client.exec_command(f"sudo tc class add dev {interface} parent 1: classid 1:1 htb rate 100mbps")
        
        mark_count = 2  # Start from 2 to reserve 1:1 as default class
        dst_node_details = exclude_src_node(source_node_name, node_details)
        source_node_index = list(node_details.keys()).index(source_node_name)

        for dst_node, details in dst_node_details.items():
            dst_node_index = list(node_details.keys()).index(dst_node)
            dst_node_ip = details['ip']
            latency = delay_matrix[source_node_index][dst_node_index]

            client.exec_command(f"sudo tc class add dev {interface} parent 1: classid 1:{mark_count} htb rate 100mbps")
            client.exec_command(f"sudo tc qdisc add dev {interface} parent 1:{mark_count} handle {mark_count}0: netem delay {latency}ms")
            client.exec_command(f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {dst_node_ip} flowid 1:{mark_count}")
            mark_count += 1
    except Exception as e:
        print(f"Failed to apply latency for {source_node_name}: {e}")
    finally:
        client.close()

def exclude_src_node(src_node_name, node_details):
    return {name: details for name, details in node_details.items() if name != src_node_name}

def automate_latency_injection(params):
    source_node_name, delay_matrix, node_details = params
    username = node_details[source_node_name]['username']
    key_path = node_details[source_node_name]['key_path']
    interface = 'eth0'  # Assuming the interface name is eth0
    apply_latency_between_nodes(source_node_name, username, key_path, interface, delay_matrix, node_details)

# (2) ############################################## Compose-post workload under different delays #############################################################
def run_single_workload(url, thread_num, connections, duration, qps, script_path, output_file):
    for q in qps:
        command = [
            "/home/ubuntu/DeathStarBench/wrk2/wrk",
            "-D", "exp",
            f"-t{thread_num}",
            f"-c{connections}",
            f"-d{duration}",
            "-L",
            "-s", script_path,
            url,
            f"-R{q}"
        ]
        print(f"Running command: {' '.join(command)}")
        
        with open(output_file, 'a') as f:
            process = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
            # No wait() here, so the next workload can start while this one runs in parallel
            print(f"Workload started for {url}")


# (3) Main function to handle parallel execution of both tasks
def main():
    node_details = {
        'k8s-worker-1': {'ip': '172.26.128.30', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-2': {'ip': '172.26.132.91', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-3': {'ip': '172.26.133.31', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-4': {'ip': '172.26.132.241', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-5': {'ip': '172.26.132.142', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-6': {'ip': '172.26.133.55', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-7': {'ip': '172.26.130.22', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-8': {'ip': '172.26.130.82', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'},
        'k8s-worker-9': {'ip': '172.26.133.118', 'username': 'ubuntu', 'key_path': '/home/ubuntu/.ssh/id_rsa'}
    }
    thread_num = 8
    connections = 100
    duration = '20m' # worklaod durations 
    # Duration for each URL = 15 minutes, changing delay every 5 minutes
    delay_interval = 5 * 60  # 5 minutes in seconds
    QPS = [50]
    script_path = "/home/ubuntu/DeathStarBench/socialNetwork/wrk2/scripts/social-network/compose-post.lua"
    urls = [
        "http://nginx-thrift.social-network.svc.cluster.local:8080/wrk2-api/post/compose",
        "http://nginx-thrift.social-network2.svc.cluster.local:8080/wrk2-api/post/compose",
        "http://nginx-thrift.social-network3.svc.cluster.local:8080/wrk2-api/post/compose"
    ]
    output_file = "/home/ubuntu/ms_scheduling/social_net/strategy/comparisions/changingWorkloads/output_result1.txt"

    

    def latency_task():
        start_time = time.time()
        while time.time() - start_time < int(duration.replace('m', '')) * 60:  # Run for {duration} minutes
            # Apply different delay matrix every intervals minutes
            if (time.time() - start_time) // delay_interval == 0: 
                delay_matrix = generate_delay_matrix(9, 0, 0) 
            elif (time.time() - start_time) // delay_interval == 1:
                delay_matrix = generate_delay_matrix(9, 5, 20)
            elif (time.time() - start_time) // delay_interval == 2:
                delay_matrix = generate_delay_matrix(9, 5, 30)
            else:
                delay_matrix = generate_delay_matrix(9, 5, 40)

            # Apply latency injection
            params_list = [(source_node, delay_matrix, node_details) for source_node in node_details.keys()]
            with Pool(processes=len(node_details)) as pool:
                pool.map(automate_latency_injection, params_list)

            time.sleep(delay_interval)  # Wait for the next interval to apply the next delay matrix

    def workload_task():
        # Use ThreadPoolExecutor to run workloads for each URL concurrently
        with ThreadPoolExecutor(max_workers=len(urls)) as executor:
            for url in urls:
                executor.submit(run_single_workload, url, thread_num, connections, duration, QPS, script_path, output_file)

    # Running both tasks in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(latency_task)  # Task for applying latency
        executor.submit(workload_task)  # Task for sending workload requests

if __name__ == '__main__':
    main()

