import os
import pickle
import pandas as pd

dataset_folder = "PICKLE_Datasets"

with open(os.path.join(dataset_folder, 'AS_dataset.pkl'), 'rb') as f:
    AS_dataset = pickle.load(f)

with open(os.path.join(dataset_folder, 'probe_dataset.pkl'), 'rb') as f:
    probe_dataset = pickle.load(f)

print(AS_dataset)

print(probe_dataset)

# Evaluate if there are limitations in the provided datasets (AS and probe data set).  If you  find  limitations,  describe  these  and  conjecture  possible  reasons,  supported  withdata.
