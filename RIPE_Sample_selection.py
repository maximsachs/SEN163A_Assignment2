"""
 For a single hour in the RIPE data set: find all valid entries where the probe has
 hosting type AS and the target IPv4 is from an EU country. Implement this in an efficient way.
 It is advisable to store the raw resultsof each file.

 IF the decompressed file is available then will use that as input, otherwise using the compressed file and decompressing on the fly.
"""

import os
import pickle
import numpy as np
import pandas as pd
import time
import bz2
import os
import sys
import json
from tqdm import tqdm
from eu_country_codes import european_country_codes
import ipaddress
import multiprocessing
from collections import deque
import humanize
import datetime as dt

start_time = time.time()
cpu_count = max(1, multiprocessing.cpu_count()-3) # You can overwrite the cpu count to 1 here to run in single process mode.
dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored
data_folder = "RIPE_Dataset" # The folder where the raw ping input data is stored.
selected_data_output_folder = "RIPE_Preprocessed_Data" # The folder to put the selected sample files.
day_to_get = "2021-02-20"
dataset_type = "ping"
n_files_to_process = 24 # Set to 1 for first file, set to 24 for all the files in the folder.
n_lines_to_process = 0 # Set to 0 or False to run the whole dataset.
ip_versions = [4] # Add a 6 to this list if you also want to analyse the ipv6.

if not os.path.exists(selected_data_output_folder):
    os.mkdir(selected_data_output_folder)

# Loading the GEO IP lookup tables.
ip_country_lookup = {}
if 4 in ip_versions:
    ipv4_locations = pd.read_csv(os.path.join("IP2LOCATION-LITE-DB1.CSV", "IP2LOCATION-LITE-DB1.CSV"), 
                                 names=["start_ip", "end_ip", "country_code", "country_long"],
                                 dtype={"start_ip": np.uint64, "end_ip": np.uint64, "country_code": str, "country_long": str})
    # Checking that there is no gaps here, because then we can be sure that all ips are covered if we simply search one column only.
    predicted_start_ip = ipv4_locations["end_ip"][:-1].values+1
    actual_start_ip = ipv4_locations["start_ip"][1:].values
    np.testing.assert_array_equal(actual_start_ip, predicted_start_ip)
    ip_country_lookup[4] = ipv4_locations
if 6 in ip_versions:
    ipv6_locations = pd.read_csv(os.path.join("IP2LOCATION-LITE-DB1.IPV6.CSV", "IP2LOCATION-LITE-DB1.IPV6.CSV"),
                                 names=["start_ip", "end_ip", "country_code", "country_long"],
                                 dtype={"start_ip": np.float128, "end_ip": np.float128, "country_code": str, "country_long": str})
    # Checking that there is no gaps here, because then we can be sure that all ips are covered if we simply search one column only.
    predicted_start_ip = ipv6_locations["end_ip"][:-1].values+1
    actual_start_ip = ipv6_locations["start_ip"][1:].values
    np.testing.assert_array_equal(actual_start_ip, predicted_start_ip)
    ip_country_lookup[6] = ipv6_locations


with open(os.path.join(dataset_folder, 'AS_in_EU_with_Probe.pkl'), 'rb') as f:
    AS_in_EU_with_Probe = pickle.load(f)
    # print(AS_in_EU_with_Probe)

# Getting a set of all the probes we want to analyse.
prbs_to_select = set(np.concatenate(AS_in_EU_with_Probe["prb_ids"].values, axis=0))

files_to_process = []

for i in range(n_files_to_process):
    # https://data-store.ripe.net/datasets/atlas-daily-dumps/2021-02-20/ping-2021-02-20T0000.bz2
    filename = f'{dataset_type}-{day_to_get}T{i:02}00'
    files_to_process.append(filename)

def perform_sampling_on_file(input_filename, shared_counter, batch_size=2500):
    """
    Does the sample selection for 1 specific file.
    """

    # IF the decompressed file is available then will use that as input,
    # otherwise using the compressed file and decompressing on the fly
    decomFilename = os.path.join(data_folder, "decompressed", input_filename)
    bz2Filename = os.path.join(data_folder, input_filename+'.bz2')
    if os.path.exists(decomFilename):
        ping_dataset_file = open(decomFilename, 'rt') 
        file_mode = "decompressed"
    elif os.path.exists(bz2Filename):
        ping_dataset_file = bz2.open(bz2Filename, 'rt')
        file_mode = "bz2"
    else:
        print(f"{input_filename} does not exist, check your settings and are is all of the RIPE data downloaded?")
        return
    
    print(f"Beginning processing of file {input_filename} in \"{file_mode}\" mode.\n")

    if file_mode == "decompressed":
        out_folder_decompressed = os.path.join(selected_data_output_folder, "decompressed")
        if not os.path.exists(out_folder_decompressed):
            os.mkdir(out_folder_decompressed)
        output_filename = os.path.join(out_folder_decompressed, input_filename)
        output_file = open(output_filename, 'w') 
    elif file_mode == "bz2":
        output_filename = os.path.join(selected_data_output_folder, input_filename+'.bz2')
        output_file = bz2.open(output_filename, 'wt') 
    tot_count = 0
    count = 0
    line_batch = ""
    if cpu_count > 1:
        the_iterator = ping_dataset_file
    else:
        the_iterator = tqdm(ping_dataset_file)
    for line in the_iterator:
        count += 1
        tot_count += 1
        # In some cases the ping sample failed due to some error, e.g. dns resolution failed.. So if we encounter error, we skip.
        if "error" in line:
            continue
        # 1. Identify which type of ip this is.
        ip_version = int(line.split("\"af\"")[-1][1:].split(",")[0])
        if ip_version in ip_versions:
            # 2. Identify if the probe_id is within the ids we want to analyse.
            # To do so we split at prb_id and take the latter half \":6851,\"timest..
            # Then remove the first two characters (":) and split at the next comma, take first entry thats the probe id.
            # Convert to int, cause we doing quickmath.
            prb_id = int(line.split("\"prb_id\"")[-1][1:].split(",")[0])
            if prb_id in prbs_to_select:
                # 3. Lookup if the destination ip is in a european country.
                # Take note of the slightly different string parsing here, because the value is a string in the input line! Previous values were integers.
                dst_addr = line.split("\"dst_addr\"")[-1][2:].split("\",")[0]
                # Converting the destination address into the decimal integer representation:
                dst_addr_int = int(ipaddress.ip_address(dst_addr))
                if ip_version == 6:
                    # Gotta convert to float here:
                    dst_addr_int = np.float128(dst_addr_int)
                # Finding which country code its in:
                geo_ip_row = ip_country_lookup[ip_version][ip_country_lookup[ip_version]["end_ip"] > dst_addr_int].iloc[0]
                country_code = geo_ip_row["country_code"]
                if country_code in european_country_codes:
                    # Adding the country code to the line:
                    line = line[:-2]+f",\"country_code\":\"{country_code}\"}}\n"
                    line_batch += line
        # Every batch_size lines we update the global counter variable.
        if count % batch_size == 0:
            shared_counter.value += batch_size
            count = 0
            output_file.write(line_batch)
        if n_lines_to_process and tot_count > n_lines_to_process:
            break

    # Cosing the files.
    ping_dataset_file.close()
    output_file.close()
    # Updating the shared counter with the remaining count:
    if line_batch:
        output_file.write(line_batch)
    shared_counter.value += count
    return

# Setting up progress monitoring.
shared_counter = multiprocessing.Manager().Value('i', 0)
last_progresses = deque([], maxlen=60)
print(f"Starting processing with {cpu_count} parallel tasks")
if cpu_count > 1:
    with multiprocessing.Pool(processes=cpu_count) as pool:
        jobs = []
        for input_filename in files_to_process:
            new_job = pool.apply_async(func=perform_sampling_on_file, args=(input_filename, shared_counter))
            jobs.append(new_job)
        pool.close()
        # Quick sleep to give the processes time to start off.
        time.sleep(1)
        avg_per_second = np.nan
        refresh_interval = 3 # Number of seconds between output updates.
        while not all([job.ready() for job in jobs]):
            last_progresses.append(shared_counter.value)
            if len(last_progresses) > 2:
                avg_per_second = np.mean(np.diff(last_progresses))/refresh_interval
            running_duration = time.time() - start_time
            human_duration = humanize.time.precisedelta(dt.timedelta(seconds=running_duration))
            progress_message = f"[{human_duration}] Processed {humanize.intword(shared_counter.value)} lines so far, lines per second: {humanize.intword(avg_per_second)}"
            sys.stdout.write("\r" + progress_message)
            sys.stdout.flush()
            time.sleep(refresh_interval)
        pool.join()
        # Printing newline so we can print normally again.
        print(f"\rProcessed {humanize.intword(shared_counter.value)} lines so far, lines per second: {humanize.intword(avg_per_second)}")
else:
    for input_filename in files_to_process:
        perform_sampling_on_file(input_filename, shared_counter)



print("Took", time.time()-start_time, "seconds")