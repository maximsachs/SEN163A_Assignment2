# -*- coding: utf-8 -*-
# Course: SEN163A
# Assignment: 2
# Date: 26 mrt 2020
# Name: Jordy Sanchez
# Description: Load RIPE data
# The RIPE data is compressed in a bz2 file. You can open it directly in python
# to save storage space, but at the expense of additional computing power 
# (for on-the-fly decompression).

import time
import bz2
import os
import sys
import json

day_to_get = "2021-02-20"
dataset_type = "ping"
data_folder = "RIPE_Dataset"
n_lines_to_test = 1000000

files_to_process = []

for i in range(1):
    # https://data-store.ripe.net/datasets/atlas-daily-dumps/2021-02-20/ping-2021-02-20T0000.bz2
    filename = f'{dataset_type}-{day_to_get}T{i:02}00'
    files_to_process.append(filename)



# OPTION 1: open decompressed file
#decompression of one file can take up to 5 minutes (7zip @ Intel i5 4210U)
decomFilename = os.path.join(data_folder, "decompressed", files_to_process[0])
decomFile     = open(decomFilename, 'rt') 

#read first line and print
firstLine = decomFile.readline();
print(firstLine)

#the line appears to be json-formatted: pretty print json
firstLineJson = json.loads(firstLine)
print(json.dumps(firstLineJson, sort_keys=True, indent=4))

#estimate total number of lines
firstLine_sizeInBytes   = sys.getsizeof(firstLine) 
decomFile_sizeInBytes   = os.stat(decomFilename).st_size
nrOfLines               = round(decomFile_sizeInBytes/firstLine_sizeInBytes)
print("\nEstimated nr of lines = " + str(nrOfLines))

#read first 100k lines to estimate total loading time
count = 0;
st    = time.time()
for line in decomFile:
    count = count + 1
    if count>n_lines_to_test: break
    
#print time and estimate total time            
dur_dec         = round(time.time() - st,2)
estTotTime  = round( (dur_dec/n_lines_to_test)*nrOfLines )
print("\nDecompressed file:" )
print("Loading 100k lines took: "  + str(dur_dec) + " seconds")
print("Estimated loading time of entire decompression file: "  + \
      str(estTotTime) + " seconds" )

#finally close decomFile
decomFile.close()

# OPTION 2: 
#open .bz2 file directly
bz2Filename = os.path.join(data_folder, files_to_process[0])+'.bz2'
print(bz2Filename)
bz2File     = bz2.open(bz2Filename, 'rt') 
print(bz2File)
#read first 100k lines to estimate total loading time
count = 0;
st    = time.time()
for line in bz2File:
    count = count + 1
    if count>n_lines_to_test: break

#print time and estimate total time            
dur_comp         = round(time.time() - st,2)
estTotTime  = round( (dur_comp/n_lines_to_test)*nrOfLines )
print("\nbz2 file:" )
print("Loading 100k lines took: "  + str(dur_comp) + " seconds")
print("Estimated loading time of entire bz2 file: "  + str(estTotTime) + \
      " seconds" )

#finally close bz2File
bz2File.close()

print("Decompressing to disk first is approx", str(dur_comp/dur_dec), "times faster than realtime")