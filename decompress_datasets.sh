# Uses parallel processing to decompress the bz2 files into the folder RIPE_Dataset/decompressed/
cd RIPE_Dataset/
mkdir -p decompressed
find . -name \*.bz2 -print0 | parallel -0 -j 6 'bzip2 -cdk {} > decompressed/{/.}'  