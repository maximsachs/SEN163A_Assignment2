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
from tqdm import tqdm
from eu_country_codes.eu_country_codes import european_country_codes
import ipaddress
import multiprocessing
from collections import deque
import humanize
import datetime as dt


if __name__ == "__main__":
    start_time = time.time()
    cpu_count = max(1, multiprocessing.cpu_count()-2)
    cpu_count=1 # You can overwrite the cpu count to 1 here to run in single process mode.
    dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored
    data_folder = "RIPE_Dataset" # The folder where the raw ping input data is stored.
    selected_data_output_folder = "RIPE_Preprocessed_Data" # The folder to put the selected sample files.
    day_to_get = "2021-02-20"
    dataset_type = "ping"
    n_files_to_process = 24 # Set to 1 for first file, set to 24 for all the files in the folder.
    n_lines_to_process = 1 # Set to 0 or False to run the whole dataset.
    use_custom_json_parser = True # When False uses normal json.load, when True, uses direct string based parsing, this is slightly faster than the json loads.
    ip_versions = [4] # Add a 6 to this list if you also want to analyse the ipv6.
    dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored

    with open(os.path.join(dataset_folder, 'AS_in_EU_with_Probe.pkl'), 'rb') as f:
        AS_in_EU_with_Probe = pickle.load(f)
        print(AS_in_EU_with_Probe)

    files_to_process = []
    for i in range(n_files_to_process):
        # https://data-store.ripe.net/datasets/atlas-daily-dumps/2021-02-20/ping-2021-02-20T0000.bz2
        filename = f'{dataset_type}-{day_to_get}T{i:02}00'
        files_to_process.append(filename)


    for input_filename in files_to_process:
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
        
        print(f"\rBeginning processing of file {input_filename} in \"{file_mode}\" mode.\n")

        if cpu_count > 1:
            the_iterator = ping_dataset_file
        else:
            the_iterator = tqdm(ping_dataset_file)
        
        tot_count = 0
        count = 0
        for line in the_iterator:
            count += 1
            tot_count += 1
            
            # TODO: Do line level processing here!
            print(line)



            if n_lines_to_process and tot_count >= n_lines_to_process:
                break