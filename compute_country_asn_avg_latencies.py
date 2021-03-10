import sys
import os
sys.path.insert(0, os.getcwd()) # So that windows can find the contry codes module!
import os
import pickle
import numpy as np
import pandas as pd
import bisect
import time
import bz2
import json
import pprint
from tqdm import tqdm
from eu_country_codes.eu_country_codes import european_country_codes
import ipaddress
import multiprocessing
from collections import deque, defaultdict
import humanize
import datetime as dt

def process_file(input_filename, shared_counter, batch_size=100000):
    """
    Goes through one file and extracts the informationof interest.
    """
    # Opening the preprocessed input files:
    decomFilename = os.path.join(selected_data_output_folder, "decompressed", input_filename)
    bz2Filename = os.path.join(selected_data_output_folder, input_filename+'.bz2')
    if os.path.exists(decomFilename):
        ping_dataset_file = open(decomFilename, 'rt') 
        file_mode = "decompressed"
    elif os.path.exists(bz2Filename):
        ping_dataset_file = bz2.open(bz2Filename, 'rt')
        file_mode = "bz2"
    else:
        print(f"{input_filename} does not exist, check your settings and are is all of the RIPE data downloaded?")
        raise
    
    print(f"\rBeginning processing of file {input_filename} in \"{file_mode}\" mode.                     \n")

    if cpu_count > 1:
        the_iterator = ping_dataset_file
    else:
        the_iterator = tqdm(ping_dataset_file)

    # Creating a default dict, so we don't have to initialise keys.
    # With three nested dicts, with default value 0.
    cumulative_latency_counter = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))
    
    tot_count = 0
    count = 0
    for line in the_iterator:
        count += 1
        tot_count += 1
        
        line_dict = json.loads(line)
        
        # Identify the AS that the probe is in, if no AS then move on.
        # This approach implicitly handles when there are multiple probes within one AS.
        asn = prb_id_to_asn.get(line_dict["prb_id"], False)
        if asn:
            country_code = line_dict["country_code"] 
            avg_latency = line_dict["avg"] 
            # In some samples, there appears to be no result, because ping never returned, then avg = -1. So:
            # e.g.: {"fw":5020,"mver":"2.2.1","lts":5,"dst_name":"185.162.183.222","af":4,"dst_addr":"185.162.183.222","src_addr":"93.174.194.183","proto":"ICMP","size":64,"result":[{"x":"*"},{"x":"*"},{"x":"*"}],"dup":0,"rcvd":0,"sent":3,"min":-1,"max":-1,"avg":-1,"msm_id":23171304,"prb_id":6658,"timestamp":1613779317,"msm_name":"Ping","from":"93.174.194.183","type":"ping","group_id":23171303,"step":240,"country_code":"PT"}
            if avg_latency > 0:
                # Increment cumulative avg, and sample count.
                cumulative_latency_counter[country_code][asn]["cumulative_avg"] += avg_latency
                cumulative_latency_counter[country_code][asn]["count"] += 1

        # Every batch_size lines we update the global counter variable.
        if count % batch_size == 0:
            shared_counter.value += batch_size
            count = 0

        if n_lines_to_process and tot_count >= n_lines_to_process:
            break

    # Cosing the files.
    ping_dataset_file.close()
    shared_counter.value += count
    # Turning default dict to normal dict:
    cumulative_latency_counter = json.loads(json.dumps(cumulative_latency_counter))
    return cumulative_latency_counter

if __name__ == "__main__":
    start_time = time.time()
    # Not using as many cores as for the previous, as we are now bottlenecked by disk read speeds. For me 3 cores is best. Limiting to at most be the number of cores in the system.
    cpu_count = min(3, multiprocessing.cpu_count())
    # cpu_count = 1 # You can overwrite the cpu count to 1 here to run in single process mode.
    dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored
    data_folder = "RIPE_Dataset" # The folder where the raw ping input data is stored.
    selected_data_output_folder = "RIPE_Preprocessed_Data" # The folder to put the selected sample files.
    day_to_get = "2021-02-20"
    dataset_type = "ping"
    n_files_to_process = 24 # Set to 1 for first file, set to 24 for all the files in the folder.
    n_lines_to_process = 0 # Set to 0 or False to run the whole dataset.
    use_custom_json_parser = True # When False uses normal json.load, when True, uses direct string based parsing, this is slightly faster than the json loads.
    dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored

    with open(os.path.join(dataset_folder, 'AS_in_EU_with_Probe.pkl'), 'rb') as f:
        AS_in_EU_with_Probe = pickle.load(f)
        print(AS_in_EU_with_Probe)

    with open(os.path.join(dataset_folder, 'probe_dataset.pkl'), 'rb') as f:
        probe_dataset = pickle.load(f)
        probe_dataset.set_index("prb_id", inplace=True)
        print(probe_dataset)

    # Selecting from the probe dataset only the probes that have an AS in the AS_in_EU_with_Probe dataset.
    probe_dataset = probe_dataset[probe_dataset["ASN"].isin(AS_in_EU_with_Probe["ASN"])]
    prb_id_to_asn = probe_dataset["ASN"].to_dict()

    files_to_process = []
    for i in range(n_files_to_process):
        # https://data-store.ripe.net/datasets/atlas-daily-dumps/2021-02-20/ping-2021-02-20T0000.bz2
        filename = f'{dataset_type}-{day_to_get}T{i:02}00'
        files_to_process.append(filename)

    # Setting up progress monitoring.
    shared_counter = multiprocessing.Manager().Value('i', 0)
    last_progresses = deque([], maxlen=60)
    print(f"Starting processing with {cpu_count} parallel tasks")

    # Processing each file
    results = []
    if cpu_count > 1:
        with multiprocessing.Pool(processes=cpu_count) as pool:
            jobs = []
            for input_filename in files_to_process:
                new_job = pool.apply_async(func=process_file, args=(input_filename, shared_counter))
                jobs.append(new_job)
            pool.close()
            # Quick sleep to give the processes time to start off.
            time.sleep(1)
            avg_per_second = np.nan
            refresh_interval = 2 # Number of seconds between output updates.
            while not all([job.ready() for job in jobs]):
                last_progresses.append(shared_counter.value)
                if len(last_progresses) > 2:
                    avg_per_second = np.mean(np.diff(last_progresses))/refresh_interval
                running_duration = time.time() - start_time
                human_duration = humanize.time.precisedelta(dt.timedelta(seconds=running_duration))
                progress_message = f"[{human_duration}] Processed {humanize.intword(shared_counter.value)} lines so far, lines per second: {humanize.intword(avg_per_second)}                            "
                sys.stdout.write("\r" + progress_message)
                sys.stdout.flush()
                time.sleep(refresh_interval)
            # Getting the results from each job and appending them to the results array.
            for job in tqdm(jobs):
                results.append(job.get())
    else:
        for input_filename in files_to_process:
            cumulative_latency_counter = process_file(input_filename, shared_counter)
            results.append(cumulative_latency_counter)
        
    # Have to combine the results from each file into the combined results matrix.
    # Creating a default dict, so we don't have to initialise keys.
    # With three nested dicts, with default value 0.
    total_cumulative_latency_counter = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))
    for result in results:
        for country_code, country_samples in result.items():
            for asn, asn_data in country_samples.items():
                total_cumulative_latency_counter[country_code][asn]["cumulative_avg"] += asn_data["cumulative_avg"]
                total_cumulative_latency_counter[country_code][asn]["count"] += asn_data["count"]

    # Calculating the actual average latencies.
    counry_asn_avg_latencies = defaultdict(lambda: defaultdict(lambda: 0))
    for country_code, country_samples in total_cumulative_latency_counter.items():
        for asn, asn_data in country_samples.items():
            counry_asn_avg_latencies[country_code][asn] = total_cumulative_latency_counter[country_code][asn]["cumulative_avg"] / total_cumulative_latency_counter[country_code][asn]["count"]

    # Turning results to a dataframe. And saving it as pickel
    df_counry_asn_avg_lat = pd.DataFrame(data=counry_asn_avg_latencies)
    print(df_counry_asn_avg_lat)
    with open(os.path.join(dataset_folder, 'country_asn_avg_latencies.pkl'), 'wb') as outfile:
        pickle.dump(df_counry_asn_avg_lat, outfile)