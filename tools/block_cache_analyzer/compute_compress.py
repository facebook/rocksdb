#!/usr/bin/env python3
import csv
import math
import os
import random
import sys


def mean(array):
   return sum(array) / len(array)

def percentile(array, p):
    index = min(int(p * len(array)), len(array) - 1)
    return array[index]


if __name__ == "__main__":
    # csv_result_dir = sys.argv[1]
    compression_ratios = {}
    compression_ratios["snappy"] = []
    compression_ratios["zstd"] = []
    compression_ratios["lz4hc"] = []
    compression_ratios["zlib"] = []
    compression_ratios["raw"] = []
    with open("sst") as sst_output:
        tmp_ratios = {}
        tmp_ratios["snappy"] = 0
        tmp_ratios["zstd"] = 0
        tmp_ratios["lz4hc"] = 0
        tmp_ratios["zlib"] = 0
        raw_size = 0
        for line in sst_output:
            if "Process /data/mysql" in line:
                # a new sst file.
                for cal in tmp_ratios:
                    if tmp_ratios[cal] != 0:
                        compression_ratios[cal].append(tmp_ratios[cal])

                tmp_ratios["snappy"] = 0
                tmp_ratios["zstd"] = 0
                tmp_ratios["lz4hc"] = 0
                tmp_ratios["zlib"] = 0
                raw_size = 0

            if "Compression: kNoCompression" in line:
                raw_size = int(line.split(" ")[-1])
                compression_ratios["raw"].append(raw_size)

            if "Compression: kSnappyCompression" in line:
                size = int(line.split(" ")[-1])
                tmp_ratios["snappy"] = float(size) / float(raw_size)

            if "Compression: kZSTD" in line:
                size = int(line.split(" ")[-1])
                tmp_ratios["zstd"] = float(size) / float(raw_size)
                if size >= 0.9 * raw_size:
                    print("poor {}".format(raw_size))
                if size <= 0.2 * raw_size:
                    print("good {}".format(raw_size))

            if "Compression: kLZ4HCCompression" in line:
                size = int(line.split(" ")[-1])
                tmp_ratios["lz4hc"] = float(size) / float(raw_size)

            if "Compression: kZlibCompression" in line:
                size = int(line.split(" ")[-1])
                tmp_ratios["zlib"] = float(size) / float(raw_size)


        for cal in tmp_ratios:
            if tmp_ratios[cal] != 0:
                compression_ratios[cal].append(tmp_ratios[cal])

    print("length: {}".format(len(compression_ratios["snappy"])))
    for cal in compression_ratios:
        compression_ratios[cal] = sorted(compression_ratios[cal])
        row = cal
        row += ","
        row += str(mean(compression_ratios[cal]))
        row += ","
        for p in range(1, 101):
            row += str(percentile(compression_ratios[cal], float(p) / float(100)))
            row += ","
        print(row)
