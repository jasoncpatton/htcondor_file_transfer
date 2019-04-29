#!/usr/bin/python

"""
Copy a given file from the source to the local execution directory and
saves it as `file0`.
This will create an additional file, `metadata`, that records the file's
original name, size, and SHA1 hash.
"""

from __future__ import print_function, unicode_literals

import os
import sys
import time
import logging
import hashlib
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("src")
    return parser.parse_args()

def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    if '_CONDOR_JOB_AD' not in os.environ:
        print("This executable must be run within the HTCondor runtime environment.")
        sys.exit(1)
    args = parse_args()

    logging.info("About to copy %s to file0", args.src)
    src_fd = open(args.src, "r")
    dest_fd = open("file0", "w")
    file_size = os.fstat(src_fd.fileno()).st_size
    logging.info("There are %.2fMB to copy", file_size / 1024. / 1024.)
    last_log = time.time()
    buf = src_fd.read(1024*1024)
    hash_obj = hashlib.sha1()
    byte_count = len(buf)
    while len(buf) > 0:
        hash_obj.update(buf)
        dest_fd.write(buf)
        buf = src_fd.read(1024*1024)
        now = time.time()
        if (now - last_log > 5):
            logging.info("Copied %.2f of %.2fMB; %.1f%% done", byte_count / 1024. / 1024., file_size / 1024. / 1024.,
                (byte_count / float(file_size)) * 100)
            last_log = now
        byte_count += len(buf)

    src_fd.close()
    logging.info("Copy complete; about to synchronize file to disk")
    os.fsync(dest_fd.fileno())
    logging.info("File synchronized to disk")
    dest_fd.close()

    logging.info("File metadata: hash=%s, size=%d", hash_obj.hexdigest(), byte_count)
    with open("metadata", "w") as metadata_fd:
        metadata_fd.write("{} {} {}\n".format(args.src, hash_obj.hexdigest(), byte_count).encode('utf-8'))

if __name__ == '__main__':
    main()
