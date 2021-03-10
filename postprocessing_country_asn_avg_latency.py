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
from eu_country_codes.eu_country_codes import european_country_codes, countries
import ipaddress
import multiprocessing
from collections import deque, defaultdict
import humanize
import datetime as dt

def asn_to_name(asn):
    return AS_in_EU_with_Probe.loc[asn, "Name"]


if __name__ == "__main__":
    start_time = time.time()
    dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored
    data_folder = "RIPE_Dataset" # The folder where the raw ping input data is stored.
    selected_data_output_folder = "RIPE_Preprocessed_Data" # The folder to put the selected sample files.
    day_to_get = "2021-02-20"
    dataset_type = "ping"
    dataset_folder = "PICKLE_Datasets" # The folder where pickle datasets are stored
    ip_versions = [4, 6] # Add a 6 to this list if you also want to analyse the ipv6.

    with open(os.path.join(dataset_folder, 'AS_in_EU_with_Probe.pkl'), 'rb') as f:
        AS_in_EU_with_Probe = pickle.load(f)
        AS_in_EU_with_Probe.set_index("ASN", inplace=True)
        print(AS_in_EU_with_Probe)
    
    avg_latencies_path = os.path.join(dataset_folder, f'country_asn_avg_latencies_ip_{"_".join([ str(i) for i in ip_versions])}.pkl')
    if not os.path.exists(avg_latencies_path):
        print(f"The requested country_asn_avg_latencies file for ip_versions {str(ip_versions)} have not been found!")
        print(f"Make sure to select the same ip_versions in compute_country_asn_avg_latencies.py")
    with open(avg_latencies_path, 'rb') as f:
        country_asn_avg_latencies = pickle.load(f)
        print(country_asn_avg_latencies)


    # If we could place one server in each country, what would the minimum average latency be for each country?
    min_latencies = country_asn_avg_latencies.min()
    min_asn = country_asn_avg_latencies.idxmin()
    asn_names = min_asn.apply(asn_to_name)
    df_mins = pd.DataFrame([min_latencies, min_asn, asn_names], index=["min latency", "ASN", "AS Name"]).transpose()
    df_mins.index = [countries.get(country_code, country_code) for country_code in df_mins.index]
    df_mins.sort_index(inplace=True)
    print(df_mins)
    print()
    print(df_mins.to_latex())
    print()


    # Since we are only allowed to place four servers,  determine the best four datacentersbased on the total latency for all countries.  Report your findings and your procedureto obtain them.  Also include the average latency for each country.

    print(country_asn_avg_latencies.isna().sum())
    # General Data cleaning step, we make the requirement that the ASN can actually reach each country.
    # Even though its definitely introducing a sampling bias, dropping all ASN for which we don't have a avg latency for each country.
    # country_asn_avg_latencies.dropna(axis=1, inplace=True)
    # print(country_asn_avg_latencies)

    # Approach 1. We just take the 4 ASN with the average best performance.
    mean_asn_performance = country_asn_avg_latencies.mean(axis=1)
    mean_asn_performance.sort_values(inplace=True)
    selected_asn_index = mean_asn_performance.index[:4]
    selected_asn = country_asn_avg_latencies.loc[selected_asn_index]
    print(selected_asn)