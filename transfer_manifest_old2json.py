#!/usr/bin/env python3

import argparse
from pathlib import Path
import shutil
import json
import os


# VERIFY_REQUEST {"name": "Arlington_2020/Field 712/20200621_15-30-48/NDRE/IMG_00228.jpg", "size": 2579152}
# VERIFY_REQUEST Madison_M1500_2020/20200613RGB/DJI_0070.JPG 7587670
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


CONVERT = {
    "VERIFY_REQUEST": verify_request,
    "TRANSFER_REQUEST": transfer_request,
    "TRANSFER_VERIFIED": transfer_verified,
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
                elif tokens[0] in ["SYNC_REQUEST"] and len(tokens) == 8:
                    f.write(line)
                elif tokens[0] in ["SYNC_DONE"] and len(tokens) == 2:
                    f.write(line)
                elif tokens[0] in ["TRANSFER_REQUEST", "VERIFY_REQUEST"]:
                    if tokens[1].startswith("{") and tokens[-1].endswith("}"):
                        try:
                            json.loads(" ".join(tokens[1:]))
                            f.write(line)
                        except json.decoder.JSONDecodeError:
                            print(f"Bad line {i+1}, skipping: {line.rstrip()}")
                            skipped_lines += 1
                    elif len(tokens) == 3 and not (
                        "{" in "".join(tokens) or "}" in "".join(tokens)
                    ):
                        newline = CONVERT[tokens[0]](*tokens[1:])
                        f.write(newline)
                    else:
                        print(f"Bad line {i+1}, skipping: {line.rstrip()}")
                        skipped_lines += 1
                elif tokens[0] in ["TRANSFER_VERIFIED"]:
                    if tokens[1].startswith("{") and tokens[-1].endswith("}"):
                        try:
                            json.loads(" ".join(tokens[1:]))
                            f.write(line)
                        except json.decoder.JSONDecodeError:
                            print(f"Bad line {i+1}, skipping: {line.rstrip()}")
                            skipped_lines += 1
                    elif len(tokens) == 5 and not (
                        "{" in "".join(tokens) or "}" in "".join(tokens)
                    ):
                        newline = CONVERT[tokens[0]](*tokens[1:])
                        f.write(newline)
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
