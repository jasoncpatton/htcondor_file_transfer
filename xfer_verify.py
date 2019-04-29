#!/usr/bin/python

"""
Verify a given file on the schedd matches the given metadata.
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
    parser.add_argument("dest")
    parser.add_argument("metadata")
    parser.add_argument("metadata_summary")
    return parser.parse_args()

def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    args = parse_args()

    with open(args.metadata, "r") as metadata_fd:
        if os.fstat(metadata_fd.fileno()).st_size > 16384:
            logging.error("Metadata file is too large")
            sys.exit(1)
        contents = metadata_fd.read()
        info = contents.strip().split()
        if len(info) != 3:
            logging.error("Metadata file format incorrect")
            sys.exit(1)
        src_fname = info[0].decode('utf-8')
        src_hexdigest = info[1]
        src_size = int(info[2])

    logging.info("About to verify contents of %s", args.dest)
    dest_fd = open(args.dest, "r")
    dest_size = os.fstat(dest_fd.fileno()).st_size
    if src_size != dest_size:
        logging.error("Copied file size (%d bytes) does not match source file size (%d bytes)", dest_size, src_size)
        sys.exit(2)
    logging.info("There are %.2fMB to verify", dest_size / 1024. / 1024.)
    last_log = time.time()
    buf = dest_fd.read(1024*1024)
    hash_obj = hashlib.sha1()
    byte_count = len(buf)
    while len(buf) > 0:
        hash_obj.update(buf)
        buf = dest_fd.read(1024*1024)
        now = time.time()
        if now - last_log > 5:
            logging.info("Verified %.2f of %.2fMB; %.1f%% done", byte_count / 1024. / 1024., dest_size / 1024. / 1024.,
                (byte_count / float(dest_size)) * 100)
            last_log = now
        byte_count += len(buf)

    dest_fd.close()
    if src_hexdigest != hash_obj.hexdigest():
        logging.info("Destination file (%s) has incorrect SHA1 digest of %s, which does not match"
            " source file %s (digest %s)", args.dest, src_hexdigest, src_fname, hash_obj.hexdigest())
        sys.exit(1)
    else:
        logging.info("File verification successful: Destination (%s) and source (%s) have matching"
            " SHA1 digest (%s)", args.dest, src_fname, src_hexdigest)

if __name__ == '__main__':
    main()
