import os
import pickle
import pandas as pd

dataset_folder = "PICKLE_Datasets"

with open(os.path.join(dataset_folder, 'AS_dataset.pkl'), 'rb') as f:
    AS_dataset = pickle.load(f)
# Sorting by AS number.
AS_dataset.sort_values("ASN", ascending=True, inplace=True)

with open(os.path.join(dataset_folder, 'probe_dataset.pkl'), 'rb') as f:
    probe_dataset = pickle.load(f)

european_country_codes = ["BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", "SE"]
print(f"There are {len(european_country_codes)} european countries:")
print(european_country_codes)

print("The As dataset:")
print(AS_dataset)
print("Country codes in the AS dataset:")
print(AS_dataset["Country"].unique())
print("The probe dataset:")
print(probe_dataset)

# Evaluate if there are limitations in the provided datasets (AS and probe data set).  If you  find  limitations,  describe  these  and  conjecture  possible  reasons,  supported  withdata.
# Limitations:
# 1. Not every ASN might have a probe located within it. Without the probe we cannot include the ASN in the analysis.
# 2. Probes might be attached within the ASN in a way that their latencies are not representative for other servers within the same ASN.

# With the AS and probe data set, find the number of AS’s that can be used forhosting in the EU and have probes in the RIPE data set. Sort the ASN’s in ascending order and include the first and last three in your report (number, name and country).
# Filtering AS with european country code
as_in_europe = AS_dataset[AS_dataset["Country"].isin(european_country_codes)]
print(f"A total of {as_in_europe.shape[0]} ASNs were found to be located in a European country.")

# Filtering AS that are in the probe id dataset.
as_in_europe_and_with_probe = as_in_europe[as_in_europe["ASN"].isin(probe_dataset["ASN"])]
as_count_per_country = as_in_europe_and_with_probe.groupby("Country").size()
print(f"A total of {as_in_europe_and_with_probe.shape[0]} ASNs were found to be located in a European country and also had a RIPE probe within them.")

# Adding the probe ids to each row:
probe_ids = []
n_probes = []
for asn in as_in_europe_and_with_probe["ASN"]:
    probes_of_asn = probe_dataset[probe_dataset["ASN"] == asn]["prb_id"].values
    probe_ids.append(probes_of_asn)
    n_probes.append(len(probes_of_asn))
as_in_europe_and_with_probe["prb_ids"] = probe_ids
as_in_europe_and_with_probe["prb_count"] = n_probes

# Saving the selected AS and their probe ids:

with open(os.path.join(dataset_folder, 'AS_in_EU_with_Probe.pkl'), 'wb') as outfile:
    pickle.dump(as_in_europe_and_with_probe, outfile)

# Printing results for report
print(as_count_per_country.to_frame().transpose())
as_in_europe_and_with_probe_for_report = as_in_europe_and_with_probe.head(3).append(as_in_europe_and_with_probe.tail(3))
print(as_in_europe_and_with_probe_for_report)
print()
print(as_in_europe_and_with_probe_for_report[["ASN", "Name", "Country", "prb_count"]].to_latex(index=False))
print()

# the probe dataset tells us in which ASN the probe is located. We can consider the performance of the probe as a proxy for the whole ASN, if there are multiple probes within the same ASN then have to take the average of the probes within the ASN for the performance.





