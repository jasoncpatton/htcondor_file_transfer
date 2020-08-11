#!/usr/bin/env python3

import argparse
from pathlib import Path
import shutil
import json
import os


# SYNC_REQUEST /home/wcic/drone_data files_at_source=83682 files_to_transfer=8 bytes_to_transfer=0 files_to_verify=83674 bytes_to_verify=387976805206 timestamp=1596727332.650157
# SYNC_REQUEST {"source_prefix": "/home/jtk/projects/htcondor_file_transfer/src", "files_at_source": 4, "files_to_transfer": 4, "bytes_to_transfer": 10289741842, "files_to_verify": 0, "bytes_to_verify": 0, "timestamp": 1596816337.0116978}
def sync_request(source_prefix, *args):
    data = {}
    data['source_prefix'] = source_prefix
    if len(args) != 6:
        raise TypeError(f"Invalid number of SYNC_REQUEST arguments passed: {len(args)}")
    for entry in args:
        key, val = entry.split('=')
        if not (key in ['files_at_source', 'files_to_transfer', 'bytes_to_transfer', 'files_to_verify', 'bytes_to_verify', 'timestamp']):
            raise TypeError(f"Invalid key in SYNC_REQUEST arguments: {key}")
        elif key in ['timestamp']:
            data[key] = float(val)
        else:
            data[key] = int(val)
    return f"SYNC_REQUEST {json.dumps(data)}\n"


# VERIFY_REQUEST Madison_M1500_2020/20200613RGB/DJI_0070.JPG 7587670
# VERIFY_REQUEST {"name": "Arlington_2020/Field 712/20200621_15-30-48/NDRE/IMG_00228.jpg", "size": 2579152}
def verify_request(name, size):
    return f"VERIFY_REQUEST {json.dumps({'name': name, 'size': int(size)})}\n"


# TRANSFER_REQUEST Arlington_2020/712W/20200628RGB/DJI_0320.JPG 7594853
# TRANSFER_REQUEST {"name": "Arlington_2020/Field 1 GXE/20200628NDRE/IMG_00001.jpg", "size": 2157739}
def transfer_request(name, size):
    return f"TRANSFER_REQUEST {json.dumps({'name': name, 'size': int(size)})}\n"


# TRANSFER_VERIFIED Madison_M1500_2020/20200613RGB/DJI_0070.JPG a28d9dee507a9fea54f4a55a495b013c050e3928 7587670 1593181879
# TRANSFER_VERIFIED {"timestamp": 1593181879, "name": "Arlington_2020/Field 712/20200621_16-13-23/NDVI/IMG_00389.jpg", "digest": "1f2cec0df3017e944e93fa56104dbfc4096a76ea", "size": 2474228}
def transfer_verified(name, digest, size, timestamp):
    return f"TRANSFER_VERIFIED {json.dumps({'name': name, 'digest': digest, 'size': int(size), 'timestamp': int(timestamp)})}\n"


# SYNC_DONE 1596776915
# SYNC_DONE {"timestamp": 1596816381.1880395}
def sync_done(timestamp):
    return f"SYNC_DONE {json.dumps({'timestamp': float(timestamp)})}\n"


CONVERT = {
    "SYNC_REQUEST": sync_request,
    "SYNC_DONE": sync_done,
    "VERIFY_REQUEST": verify_request,
    "TRANSFER_REQUEST": transfer_request,
    "TRANSFER_VERIFIED": transfer_verified,
}


TOKENS = {
    "SYNC_REQUEST": 8,
    "TRANSFER_REQUEST": 3,
    "VERIFY_REQUEST": 3,
    "TRANSFER_VERIFIED": 5,
    "SYNC_DONE": 2,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "old", type=Path, help="Path to old-style transfer_manifest.txt"
    )
    args = parser.parse_args()

    old = args.old.resolve()
    bak = old.with_suffix(".bak")
    tmp = old.with_suffix(".tmp")
    shutil.copy(old, bak)

    skipped_lines = 0
    try:
        with open(tmp, mode="w") as f:
            for i, line in enumerate(old.open()):
                tokens = line.rstrip().split()
                if len(tokens) < 1:
                    print(f"Bad line {i+1}, skipping: {line.rstrip()}")
                    skipped_lines += 1
                elif tokens[0] in TOKENS:
                    if tokens[1].startswith("{") and tokens[-1].endswith("}"):
                        try:
                            json.loads(" ".join(tokens[1:]))
                            f.write(line)
                        except Exception as e:
                            print(f"Bad line {i+1}, raised {type(e).__name__}: {str(e)}, skipping: {line.rstrip()}")
                            skipped_lines += 1
                    elif len(tokens) == TOKENS[tokens[0]] and not (
                        "{" in "".join(tokens) or "}" in "".join(tokens)
                    ):
                        try:
                            json_line = CONVERT[tokens[0]](*tokens[1:])
                            f.write(json_line)
                        except Exception as e:
                            print(f"Bad line {i+1}, raised {type(e).__name__}: {str(e)}, skipping: {line.rstrip()}")
                            skipped_lines += 1
                    else:
                        print(f"Bad line {i+1}, skipping: {line.rstrip()}")
                        skipped_lines += 1
                else:
                    print(f"Bad line {i+1}, skipping: {line.rstrip()}")
                    skipped_lines += 1
            f.flush()
            os.fsync(f)
    except Exception as e:
        tmp.unlink()
        print(f"Error on line {i+1}:")
        print(f"{line.rstrip()}")
        raise e
    else:
        tmp.rename(old)
        print(f"Skipped {skipped_lines} lines out of {i+1} lines")
