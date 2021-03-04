# Assignment 2 SEN163A
https://atlas.ripe.net/docs/data_struct/#v5000

## Dependencies

```
conda create -n sen163a python=3.8
conda activate sen163a
pip install wget
```

For the parallel extraction script on Linux install:

```
sudo apt install parallel
```

## How to use:

1.  Set up the virtual environment and install the required python packages.
2.  Run the RIPE_downloader.py, which downloads the RIPE ping dataset for the day in question and stores them in the RIPE_Dataset folder.
3.  Extract each RIPE file, this is to allow quicker processing in the following steps. To do so (on LINUX) run the decompress_datasets.sh script, which will extract the datasets using 6 parallel processes (modify the script to your number of cores). On Windows use some other bz2 extraction of your choice. The extracted files will be stored in the decompressed folder inside of the RIPE_Dataset folder.
4.  The first analysis is executed using the as_probe_analysis.py, which outputs the information necessary to answer question 1 of the assignment.