# Assignment 2 SEN163A
https://atlas.ripe.net/docs/data_struct/#v5000

## Dependencies

Make sure conda is installed correctly.

```
conda create -n sen163a python=3.8
conda activate sen163a
pip install wget pandas numpy humanize
```

For the parallel extraction script on Linux install:

```
sudo apt install parallel
```

## How to use:

Run the `I_want_it_all.sh` bash script to run the whole analysis automatically. Or go step by step with the instructions given below:

1.  Set up the virtual environment and install the required python packages.
2.  Run the `RIPE_downloader.py`, which downloads the RIPE ping dataset for the day in question and stores them in the RIPE_Dataset folder. The input files will take up around 27GB.
3.  (OPTIONAL) Extract each RIPE file, this is to allow quicker processing in the following steps. To do so run the decompress_datasets.sh script, which will extract the datasets using 6 parallel processes (modify the script to your number of cores!). If it doesn't work on Windows use some other bz2 extraction of your choice. The extracted files will be stored in the `RIPE_Dataset/decompressed` folder inside of the `RIPE_Dataset` folder. If extraction is not possible, e.g. due to hardware limitations, then simply skip this step and the following scripts will decompress the files on the fly. The decompressed file will be around 240 GB. And for the sample selection expect another 22GB of storage space to be used as well.
4.  The first analysis is executed using the `as_probe_analysis.py`, which prints the information necessary to answer question 1 of the assignment. It also saves a new pickle dataframe to the PICKLE Datasets folder, which contains the selected AS that are in EU and have at least one (1) probe.
5.  Next execute the `RIPE_Sample_selection.py` script. This will filter the ping samples for the probes of interest and to ensure that they sample a european ip. (As we are interested in the hosting performance in the European Union only.) This script can use multiprocessing as well. By default uses the bigger of 1 or 2 less than the number of cpu cores in the machine. Each process performs sample selection on 1 file at a time. In my case, processing the 559.0 million lines in bz2 (compressed) mode took 1 hour, 13 minutes and 14.37 seconds.
6.  (OPTIONAL) If up to now you were running in compressed mode, then you can decompress the preprocessed files here, if you want, using the `decompress_datasets_preprocessed.sh` shell script. This will give a small speed boost to the following steps. The compressed files should be in total around 2.1 GB big, and decompressed they will take up around 22GB.