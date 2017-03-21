#!/bin/bash

# Make Raw Folder (If Not Exist)
# mkdir -p raw

# Download Dataset
# echo "Downloading Dataset..."
# wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Books_5.json.gz
# mv reviews_Books_5.json.gz raw/

echo "Downloading Metadata..."
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Books.json.gz
mv meta_Books.json.gz raw/

# Put File Onto HDFS
# hdfs dfs -put raw/reviews_Books_5.json.gz
hdfs dfs -put raw/meta_Books.json.gz

echo "Done"
