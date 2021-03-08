"""
 For a single hour in the RIPE data set: find all valid entries where the probe has
 hosting type AS and the target IPv4 is from an EU country. Implement this in an efficient way.
 It is advisable to store the raw resultsof each file
"""

import os
import pickle
import pandas as pd


dataset_folder = "PICKLE_Datasets"

with open(os.path.join(dataset_folder, 'AS_in_EU_with_Probe.pkl'), 'rb') as f:
    AS_in_EU_with_Probe = pickle.load(f)

print(AS_in_EU_with_Probe)