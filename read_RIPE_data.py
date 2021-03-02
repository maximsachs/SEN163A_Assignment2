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

# OPTION 1: open decompressed file
#decompression of one file can take up to 5 minutes (7zip @ Intel i5 4210U)
decomFilename = 'ping-2020-02-20T0000'
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
    if count>100000: break
    
#print time and estimate total time            
dur         = round(time.time() - st,2)
estTotTime  = round( (dur/100000)*nrOfLines )
print("\nDecompressed file:" )
print("Loading 100k lines took: "  + str(dur) + " seconds")
print("Estimated loading time of entire decompression file: "  + \
      str(estTotTime) + " seconds" )

#finally close decomFile
decomFile.close()

#%% OPTION 2: 
#open .bz2 file directly
bz2Filename = 'ping-2020-02-20T0000.bz2'
bz2File     = bz2.open(bz2Filename, 'rt') 

#read first 100k lines to estimate total loading time
count = 0;
st    = time.time()
for line in bz2File:
    count = count + 1
    if count>100000: break

#print time and estimate total time            
dur         = round(time.time() - st,2)
estTotTime  = round( (dur/100000)*nrOfLines )
print("\nbz2 file:" )
print("Loading 100k lines took: "  + str(dur) + " seconds")
print("Estimated loading time of entire bz2 file: "  + str(estTotTime) + \
      " seconds" )

#finally close bz2File
bz2File.close()