'''
Amazon Dataset - Utility Function
Various utility functions useful for working with the dataset

Source: http://jmcauley.ucsd.edu/data/amazon/links.html
'''
import gzip
import json
import struct
import pandas as pd

# Read Dataset
def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)

# Convert gzip file to 'strict' json
def convertJSON(path, output):
    f = open(output, 'w')
    for l in parse(path):
        f.write(l + '\n')
    f.close()

# Construct Pandas Dataframe
def getDataFrame(path):
  i = 0
  df = {}
  for d in parse(path):
      df[i] = d
      i += 1
  return pd.DataFrame.from_dict(df, orient='index')

# Read Image Feature Data
def readImageFeatures(path):
    f = open(path, 'rb')
    while True:
        asin = f.read(10)
        if asin == '': break
        feature = []
        for i in range(4096):
            feature.append(struct.unpack('f', f.read(4)))
        yield asin, feature
