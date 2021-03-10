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
    print("Dump approach, just the 4 networks with best average performance")
    print(selected_asn)
    print()

    # Approach 2. K-Center
    print("Smort approach, doing educated guesses for networks to use.")
    acceptable_num_countries_missed = 2
    iterations = 10000
    max_search_depth = 10000
    potential_networks = {}
    n_networks = 4 # How many networks to select.

    # Could potentially removing outlier countries from the analysis.
    countries_to_exclude = [] # "MT"
    for country_code in countries_to_exclude:
        country_asn_avg_latencies.drop(country_code, axis=1, inplace=True)
    # Setting the max allowed latency to be the worst one of the countries.
    max_latency = country_asn_avg_latencies.min().max()
    # Make sure that each country in europe is actually contained in our dataset:
    unreachables = set(european_country_codes).difference(set(country_asn_avg_latencies.columns.values))
    if unreachables:
        unreachables_long = [countries.get(country_code, country_code) for country_code in unreachables]
        print(f"The following countries cannot be reached {dict(zip(unreachables, unreachables_long))}, since they had no ping samples.")
        european_country_codes = list(set(european_country_codes).intersection(set(country_asn_avg_latencies.columns.values)))

    for it in tqdm(range(iterations)):
        # We want to have a selection of networks that reaches each country in EU
        countries_not_yet_reached = european_country_codes.copy()
        # We shuffle the order of countries to reach randomly.
        np.random.shuffle(countries_not_yet_reached)
        selected_networks = []
        # We repeat the sampling process until we found a set that reaches all countries:
        depth = 0
        while len(countries_not_yet_reached) > acceptable_num_countries_missed:
            depth += 1
            while len(selected_networks) < n_networks:
                # As long as we have not yet reached the maximum number of networks, do:
                # Pick a random country that we haven't reached yet:
                country_code = countries_not_yet_reached.pop()
                # For this random country, we try out what would happen if we pick the best network for this country:
                best_network = country_asn_avg_latencies[country_code].idxmin()
                # Then we get the row for this network:
                network_row = country_asn_avg_latencies.loc[best_network]
                # For each country identify if it is below our max latency threshold:
                reached_countries = network_row[network_row <= max_latency].index.values
                countries_not_yet_reached = list(set(countries_not_yet_reached).difference(set(reached_countries)))
                np.random.shuffle(countries_not_yet_reached)
                # We will have reached at least 1 country with this network, adding it to our selection:
                selected_networks.append(best_network)
                if len(countries_not_yet_reached) == 0:
                    # If we have no more countries to reach we can prematurely break here!
                    break
            else:
                break
            if depth >= max_search_depth:
                break

        if len(countries_not_yet_reached) <= acceptable_num_countries_missed:
            # We have not reached the goal of reaching all countries with this set of networks:
            network_set_label = ",".join(sorted(selected_networks))
            if not network_set_label in potential_networks:
                selected_asn = country_asn_avg_latencies.loc[selected_networks]
                selected_performance_per_country = selected_asn.min()
                eu_avg_performance = selected_performance_per_country.mean()
                potential_networks[network_set_label] = {"selected_networks": selected_networks, "eu_avg_performance": eu_avg_performance, "countries_missed": countries_not_yet_reached, "n_countries_missed": len(countries_not_yet_reached)}

    # Finding the best from all the potential networks:
    df_potential_networks = pd.DataFrame(potential_networks).transpose()
    df_potential_networks.index = list(range(df_potential_networks.shape[0]))
    df_potential_networks.sort_values("eu_avg_performance", inplace=True)

    for n_countries_missed in df_potential_networks["n_countries_missed"].unique():
        df_potential_networks_with_missed = df_potential_networks[df_potential_networks["n_countries_missed"] == n_countries_missed]
        if df_potential_networks_with_missed.shape[0] > 0:
            best_row_with_missed = df_potential_networks_with_missed.iloc[0]
            selected_asn_with_missed = country_asn_avg_latencies.loc[best_row_with_missed["selected_networks"]]
            selected_performance_per_country_with_missed = selected_asn_with_missed.min().to_frame().transpose()
            print(f"\nWhen allowing at most {n_countries_missed} country to be missed, then following is top 5 best performance:")
            print(df_potential_networks_with_missed.head())
            print("Per country performance for the best combination of networks", best_row_with_missed["selected_networks"])
            print(selected_performance_per_country_with_missed)


