import wget
import os
import sys


# https://data-store.ripe.net/datasets/atlas-daily-dumps/2021-02-20/

day_to_get = "2021-02-20"
dataset_type = "ping"
data_folder = "RIPE_Dataset"

if not os.path.exists(data_folder):
    os.makedirs(data_folder)

files_to_get = []
for i in range(24):
    # https://data-store.ripe.net/datasets/atlas-daily-dumps/2021-02-20/ping-2021-02-20T0000.bz2
    url = f'https://data-store.ripe.net/datasets/atlas-daily-dumps/{day_to_get}/{dataset_type}-{day_to_get}T{i:02}00.bz2'
    files_to_get.append(url)

def bar_progress(current, total, width=80):
    progress_message = "Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total)
    # Don't use print() as it will print in new line every time.
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

files_already_downloaded = os.listdir(data_folder)
for file_url in files_to_get:
    filename = file_url.split("/")[-1]
    if not filename in files_already_downloaded:
        print(f"\nDownloading {filename}")
        wget.download(file_url, out=os.path.abspath(data_folder), bar=bar_progress)
    else:
        print(f"\nFile {filename} has already been downloaded")