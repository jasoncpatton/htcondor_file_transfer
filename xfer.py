#!/usr/bin/env python3

"""
Utilize HTCondor to transfer / synchronize a directory from a source on an
execute host to a local destination on the submit host.
"""

import argparse
import contextlib
import hashlib
import logging
import os
import sys
import json
import time
from pathlib import Path

import htcondor
import htcondor.dags as dags
import classad


KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30
TB = 2 ** 40

METADATA_FILE_SIZE_LIMIT = 16 * KB
SANDBOX_FILE_NAME = "file-for-xfer"

THIS_FILE = Path(__file__).absolute()


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    sync = subparsers.add_parser("sync")
    sync.add_argument("src")
    sync.add_argument("dest")
    sync.add_argument(
        "--working-dir",
        help="Directory to place working HTCondor files.",
        default="./scratch_dir",
        dest="working_dir",
    )
    sync.add_argument(
        "--requirements",
        help="Submit file requirements (e.g. 'UniqueName == \"MyLab0001\"')",
    )
    sync.add_argument(
        "--requirements_file", help="File containing submit file requirements"
    )
    sync.add_argument(
        "--unique-id",
        help="Do not submit if jobs with UniqueId already found in queue",
        dest="unique_id",
    )
    sync.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
        dest="test_mode",
    )

    generate = subparsers.add_parser("generate")
    generate.add_argument("src")
    generate.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
        dest="test_mode",
    )

    subdag = subparsers.add_parser("write_subdag")
    subdag.add_argument("source_prefix")
    subdag.add_argument("source_manifest")
    subdag.add_argument("dest_prefix")
    subdag.add_argument("dest_manifest")
    subdag.add_argument("transfer_manifest")
    subdag.add_argument("--requirements", help="Submit file requirements")
    subdag.add_argument(
        "--requirements_file", help="File containing submit file requirements"
    )
    subdag.add_argument(
        "--unique-id", help="Set UniqueId in submitted jobs", dest="unique_id"
    )
    subdag.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
        dest="test_mode",
    )

    exec = subparsers.add_parser("exec")
    exec.add_argument("src")

    verify_remote = subparsers.add_parser("verify_remote")
    verify_remote.add_argument("src")

    verify = subparsers.add_parser("verify")
    # SCRIPT POST xfer_{name} {xfer_py} verify {dest_prefix} {dest} {src_file_noslash}.metadata {transfer_manifest}
    # verify.add_argument("dest_prefix")
    # verify.add_argument("dest")
    # verify.add_argument("metadata")
    # verify.add_argument("metadata_summary")
    verify.add_argument("--json", dest="json")
    verify.add_argument("--fileid", dest="fileid")

    analyze = subparsers.add_parser("analyze")
    analyze.add_argument("transfer_manifest")

    return parser.parse_args()


def read_requirements_file(requirements_file):
    if requirements_file is None:
        return None

    return Path(requirements_file).read_text().strip()


def generate_file_listing(src, manifest_path, test_mode=False):
    manifest_path = Path(manifest_path)
    with manifest_path.open(mode="w") as f:
        for entry in walk(src):
            size = entry.stat().st_size

            if test_mode and size > 50 * MB:
                continue

            info = {"name": entry.path, "size": size}
            f.write(f"{json.dumps(info)}\n")


def walk(path):
    for entry in os.scandir(path):
        if entry.is_dir():
            yield from walk(entry.path)
        elif entry.is_file():
            yield entry


def submit_parent_dag(
    working_dir,
    source_dir,
    dest_dir,
    requirements=None,
    test_mode=False,
    unique_id=None,
):
    working_dir = Path(working_dir).absolute()
    dest_dir = Path(dest_dir).absolute()

    transfer_manifest_path = dest_dir / "transfer_manifest.txt"

    parent_dag = dags.DAG()

    parent_dag.layer(
        name="calc_work",
        submit_description=htcondor.Submit(
            {
                "universe": "vanilla",
                "executable": THIS_FILE.as_posix(),
                "output": "calc_work.out",
                "error": "calc_work.err",
                "log": "calc_work.log",
                "arguments": f"generate {source_dir} {'--test-mode' if test_mode else ''}",
                "should_transfer_files": "yes",
                "requirements": requirements if requirements is not None else "True",
                "My.IsTransferJob": "true",
                "My.UniqueID": f"{classad.quote(unique_id) if unique_id is not None else ''}",
                "My.WantFlocking": "true",
                "keep_claim_idle": "300",
                "request_disk": "1GB",
            }
        ),
        post=dags.Script(
            executable=THIS_FILE,
            arguments=[
                "write_subdag",
                source_dir,
                "source_manifest.txt",
                dest_dir,
                "destination_manifest.txt",
                transfer_manifest_path,
                "--requirements_file=requirements.txt"
                if requirements is not None
                else "",
                "--unique-id={}".format(unique_id) if unique_id is not None else "",
                "--test-mode" if test_mode else "",
            ],
        ),
    ).child_subdag(
        name="inner",
        dag_file=working_dir / "inner.dag",
        post=dags.Script(
            executable=THIS_FILE, arguments=["analyze", transfer_manifest_path]
        ),
    )

    if requirements:
        (working_dir / "requirements.txt").write_text(requirements)

    outer_dag_file = dags.write_dag(
        parent_dag, dag_dir=working_dir, dag_file_name="outer.dag"
    )

    sub = htcondor.Submit.from_dag(str(outer_dag_file))

    with change_dir(working_dir):
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            return sub.queue(txn)


@contextlib.contextmanager
def change_dir(dir):
    original = os.getcwd()
    os.chdir(dir)
    yield
    os.chdir(original)


def parse_manifest(prefix, manifest_path, log_name):
    manifest_path = Path(manifest_path)
    prefix = os.path.normpath(prefix)
    files = {}
    with manifest_path.open(mode="r") as fd:
        for line in fd:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if line.startswith("{"):
                info = json.loads(line)
                if "name" not in info:
                    raise Exception(
                        "Manifest line missing 'name' key.  Current line: %s" % line
                    )
                fname = info["name"]
                if "size" not in info:
                    raise Exception(
                        "Manifest line missing 'size' key.  Current line: %s" % line
                    )
                size = int(info["size"])
            else:
                info = line.strip().split()
                if len(info) != 2:
                    raise Exception(
                        "Manifest lines must have two columns.  Current line: %s" % line
                    )
                fname = info[0]
                size = int(info[1])
            if not fname.startswith(prefix):
                logging.error(
                    "%s file (%s) does not start with specified prefix", log_name, fname
                )
            fname = fname[len(prefix) + 1 :]
            if not fname:
                logging.warning(
                    "%s file, stripped of prefix (%s), is empty", log_name, prefix
                )
                continue
            files[fname] = size
    return files


def write_subdag(
    source_prefix,
    source_manifest,
    dest_prefix,
    dest_manifest,
    transfer_manifest_path,
    requirements=None,
    test_mode=False,
    unique_id=None,
):
    src_files = parse_manifest(source_prefix, source_manifest, "Source")

    generate_file_listing(dest_prefix, "destination_manifest.txt")
    dest_files = parse_manifest(dest_prefix, "destination_manifest.txt", "Destination")

    files_to_xfer = set()
    for fname in src_files:
        if src_files[fname] != dest_files.get(fname, -1):
            files_to_xfer.add(fname)

    transfer_manifest_path = Path(os.path.join(dest_prefix, "transfer_manifest.txt"))
    transfer_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    transfer_manifest_path.touch(exist_ok=True)

    # TODO: WHAT DOES THIS DO?
    # seems like it makes sure we don't re-verify files that have already been verified
    # ... which means we never transfer files that have already been transferred by name
    # even if they may have changed at the source!
    files_verified = set()
    with transfer_manifest_path.open(mode="r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            info = line.split()

            if info[0] != "TRANSFER_VERIFIED":
                continue

            if info[1] == "{":
                info = json.loads(" ".join(info[1:]))
                if "name" not in info or "digest" not in info or "size" not in info:
                    continue
            elif len(info) != 5:
                continue
            else:
                fname, hexdigest, size = info[1:-1]

            relative_fname = fname
            files_verified.add(relative_fname)

    files_to_verify = set()
    for fname in src_files:
        if fname in files_to_xfer:
            continue

        if fname not in files_verified:
            files_to_verify.add(fname)

    inner_dag = dags.DAG(
        max_jobs_by_category={"TRANSFER_JOBS": 1} if test_mode else None
    )

    dest_dirs = set()
    cmd_info = []
    for idx, fname in enumerate(sorted(files_to_xfer)):
        src_file = os.path.join(source_prefix, fname)
        src_file_noslash = fname.replace("/", "_SLASH_").replace(" ", "_SPACE_")
        dest = os.path.join(dest_prefix, fname)
        dest_dirs.add(os.path.split(dest)[0])

        logging.info("File transfer to perform: %s->%s", src_file, dest)

        cmd_info.append(
            {
                "src_file": src_file,
                "src_file_noslash": src_file_noslash,
                "dest": dest,
                "transfer_manifest": str(transfer_manifest_path),
                "dest_prefix": dest_prefix,
            }
        )

    with open("xfer_commands.json", "w") as cmd_fp:
        json.dump(dict(enumerate(cmd_info)), cmd_fp)

    inner_dag.layer(
        name="xfer",
        submit_description=htcondor.Submit(
            {
                "universe": "vanilla",
                "executable": THIS_FILE.as_posix(),
                "output": "$(src_file_noslash).out",
                "error": "$(src_file_noslash).err",
                "log": "xfer_file.log",
                "arguments": classad.quote("exec '$(src_file)'"),
                "should_transfer_files": "yes",
                "transfer_output_files": f"{SANDBOX_FILE_NAME}, metadata",
                "transfer_output_remaps": classad.quote(
                    f"{SANDBOX_FILE_NAME} = $(dest); metadata = $(src_file_noslash).metadata"
                ),
                "requirements": requirements if requirements is not None else "True",
                "My.IsTransferJob": "true",
                "My.UniqueID": f"{classad.quote(unique_id) if unique_id is not None else ''}",
                "My.WantFlocking": "true",
                "keep_claim_idle": "300",
                "request_disk": "1GB",
            }
        ),
        vars=cmd_info,
        post=dags.Script(
            executable=THIS_FILE,
            arguments=["verify", "--json=xfer_commands.json", "--fileid", "$JOB"],
        ),
    )

    cmd_info = []
    for fname in files_to_verify:
        src_file = os.path.join(source_prefix, fname)
        src_file_noslash = fname.replace("/", "_SLASH_").replace(" ", "_SPACE_")
        dest = os.path.join(dest_prefix, fname)

        logging.info("File to verify: %s", src_file)

        cmd_info.append(
            {
                "src_file": src_file,
                "src_file_noslash": src_file_noslash,
                "dest": dest,
                "transfer_manifest": transfer_manifest_path,
                "dest_prefix": dest_prefix,
            }
        )

    with open("verify_commands.json", "w") as cmd_fp:
        json.dump(dict(enumerate(cmd_info)), cmd_fp)

    inner_dag.layer(
        name="verify",
        submit_description=htcondor.Submit(
            {
                "universe": "vanilla",
                "executable": THIS_FILE.as_posix(),
                "output": "$(src_file_noslash).out",
                "error": "$(src_file_noslash).err",
                "log": "verify_file.log",
                "arguments": classad.quote(f"verify_remote '$(src_file)'"),
                "should_transfer_files": "yes",
                "transfer_output_files": "metadata",
                "transfer_output_remaps": classad.quote(
                    "metadata = $(src_file_noslash).metadata"
                ),
                "requirements": requirements if requirements is not None else "True",
                "My.IsTransferJob": "true",
                "My.UniqueID": f"{classad.quote(unique_id) if unique_id is not None else ''}",
                "My.WantFlocking": "true",
                "keep_claim_idle": "300",
                "request_disk": "1GB",
            }
        ),
        vars=cmd_info,
        post=dags.Script(
            executable=THIS_FILE,
            arguments=["verify", "--json=verify_commands.json", "--fileid", "$JOB"],
        ),
    )

    dags.write_dag(inner_dag, dag_dir=Path.cwd(), dag_file_name="inner.dag")

    for dest_dir in dest_dirs:
        Path(dest_dir).mkdir(exist_ok=True, parents=True)

    bytes_to_transfer = sum(src_files[fname] for fname in files_to_xfer)
    bytes_to_verify = sum(src_files[fname] for fname in files_to_verify)
    with transfer_manifest_path.open(mode="a") as f:
        f.write(
            f"SYNC_REQUEST {source_prefix} files_at_source={len(src_files)} files_to_transfer={len(files_to_xfer)} bytes_to_transfer={bytes_to_transfer} files_to_verify={len(files_to_verify)} bytes_to_verify={bytes_to_verify} timestamp={time.time()}\n"
        )

        for fname in files_to_xfer:
            info = {"name": fname, "size": src_files[fname]}
            f.write("TRANSFER_REQUEST {}\n".format(json.dumps(info)))
        for fname in files_to_verify:
            info = {"name": fname, "size": src_files[fname]}
            f.write("VERIFY_REQUEST {}\n".format(json.dumps(info)))


def xfer_exec(src_path):
    src_path = Path(src_path)

    if "_CONDOR_JOB_AD" not in os.environ:
        print("This executable must be run within the HTCondor runtime environment.")
        sys.exit(1)

    dst_path = Path(os.environ["_CONDOR_SCRATCH_DIR"]) / SANDBOX_FILE_NAME

    logging.info("About to copy %s to %s", src_path, dst_path)

    file_size = src_path.stat().st_size
    logging.info("There are %.2f MB to copy", file_size / MB)
    last_log = time.time()

    hash_obj = hashlib.sha1()

    with src_path.open(mode="rb") as src, dst_path.open(mode="wb") as dst:
        buf = src.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hash_obj.update(buf)
            dst.write(buf)

            buf = src.read(MB)

            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Copied %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    file_size / MB,
                    (byte_count / float(file_size)) * 100,
                )
                last_log = now

            byte_count += len(buf)

        logging.info("Copy complete; about to synchronize file to disk")

        os.fsync(dst.fileno())

        logging.info("File synchronized to disk")

    logging.info("File metadata: hash=%s, size=%d", hash_obj.hexdigest(), byte_count)

    with open("metadata", "w") as metadata_fd:
        info = {
            "name": str(src_path),
            "digest": hash_obj.hexdigest(),
            "size": byte_count,
        }
        metadata_fd.write("{}\n".format(json.dumps(info)))


def verify_remote(src):
    src = Path(src)

    if "_CONDOR_JOB_AD" not in os.environ:
        print("This executable must be run within the HTCondor runtime environment.")
        sys.exit(1)

    logging.info("About to verify %s", src)

    file_size = src.stat().st_size

    logging.info("There are %.2f MB to verify", file_size / MB)
    last_log = time.time()

    hash_obj = hashlib.sha1()

    with src.open(mode="rb") as src_fd:
        buf = src_fd.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hash_obj.update(buf)
            buf = src_fd.read(MB)
            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Copied %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    file_size / MB,
                    (byte_count / file_size) * 100,
                )
                last_log = now
            byte_count += len(buf)

    logging.info("Checksum computation complete")

    logging.info("File metadata: hash=%s, size=%d", hash_obj.hexdigest(), byte_count)

    with open("metadata", "w") as metadata_fd:
        info = {"name": src, "digest": hash_obj.hexdigest(), "size": byte_count}
        metadata_fd.write(f"{json.dumps(info)}\n")


def verify(dest_prefix, dest, metadata_path, metadata_summary):
    dest_prefix = Path(dest_prefix)
    dest = Path(dest)
    metadata_path = Path(metadata_path)
    metadata_summary = Path(metadata_summary)

    if metadata_path.stat().st_size > 16384:
        logging.error("Metadata file is too large")
        sys.exit(1)

    contents = metadata_path.read_text().strip()

    if not contents:
        logging.error("Metadata file is empty")
        sys.exit(1)

    info = json.loads(contents)

    if any(key not in info for key in {"name", "digest", "size"}):
        logging.error("Metadata file format incorrect; missing keys")
        sys.exit(1)

    src_fname = info["name"]
    src_hexdigest = info["digest"]
    src_size = int(info["size"])

    relative_fname = dest.relative_to(dest_prefix)

    logging.info("About to verify contents of %s", dest)

    dest_size = dest.stat().st_size

    if src_size != dest_size:
        logging.error(
            "Copied file size (%d bytes) does not match source file size (%d bytes)",
            dest_size,
            src_size,
        )
        sys.exit(2)

    logging.info("There are %.2f MB to verify", dest_size / MB)
    last_log = time.time()

    hash_obj = hashlib.sha1()

    with dest.open(mode="rb") as dest_fd:
        buf = dest_fd.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hash_obj.update(buf)
            buf = dest_fd.read(MB)

            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Verified %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    dest_size / MB,
                    (byte_count / dest_size) * 100,
                )
                last_log = now

            byte_count += len(buf)

    dest_hexdigest = hash_obj.hexdigest()
    if src_hexdigest != dest_hexdigest:
        logging.info(
            "Destination file (%s) has incorrect SHA1 digest of %s, which does not match"
            " source file %s (digest %s)",
            dest,
            dest_hexdigest,
            src_hexdigest,
            src_fname,
        )
        sys.exit(1)

    logging.info(
        "File verification successful: Destination (%s) and source (%s) have matching"
        " SHA1 digest (%s)",
        dest,
        src_fname,
        src_hexdigest,
    )

    with metadata_summary.open(mode="a") as md_fd:
        info = {
            "name": str(relative_fname),
            "digest": src_hexdigest,
            "size": src_size,
            "timestamp": int(time.time()),
        }
        md_fd.write(f"TRANSFER_VERIFIED {json.dumps(info)}\n")
        os.fsync(md_fd.fileno())

    metadata_path.unlink()
    if metadata_path.suffix == ".metadata":
        out_file = metadata_path.with_suffix(".out")
        if out_file.exists():
            out_file.unlink()
        err_file = metadata_path.with_suffix(".err")
        if err_file.exists():
            err_file.unlink()


def analyze(transfer_manifest):
    transfer_manifest = Path(transfer_manifest)

    sync_request_start = None
    sync_request = {"files": {}, "xfer_files": set(), "verified_files": {}}
    dest_dir = os.path.abspath(os.path.split(transfer_manifest)[0])
    sync_count = 0

    with open(transfer_manifest, "r") as fp:
        for idx, line in enumerate(transfer_manifest.open(mode="r")):
            info = line.strip().split()
            # Format: SYNC_REQUEST {} files_at_source={} files_to_transfer={} bytes_to_transfer={} files_to_verify={} bytes_to_verify={} timestamp={}
            if info[0] == "SYNC_REQUEST":
                sync_count += 1
                # if sync_request_start is not None:
                #    logging.error("Sync request started at line %d but never finished; inconsistent log",
                #        sync_request_start)
                #    sys.exit(4)
                sync_request_start = idx
                for entry in info[2:]:
                    key, val = entry.split("=")
                    if key == "timestamp":
                        continue
                    sync_request[key] = int(val)
            # Format: TRANSFER_REQUEST fname size
            elif info[0] == "TRANSFER_REQUEST" or info[0] == "VERIFY_REQUEST":
                if sync_request_start is None:
                    logging.error(
                        "Transfer request found at line %d before sync started; inconsistent log",
                        idx,
                    )
                    sys.exit(4)

                local_info = json.loads(" ".join(info[1:]))
                size = int(local_info["size"])
                fname = local_info["name"]

                # File was previously verified.
                if sync_request["verified_files"].get(fname, None) == size:
                    continue
                sync_request["files"][fname] = size
                if info[0] == "TRANSFER_REQUEST":
                    if info[1][0] == "{":
                        local_info = json.loads(" ".join(info[1:]))
                        sync_request["xfer_files"].add(local_info["name"])
                    else:
                        sync_request["xfer_files"].add(info[1])
            # Format: TRANSFER_VERIFIED relative_fname hexdigest size timestamp:
            elif info[0] == "TRANSFER_VERIFIED":
                if sync_request_start is None:
                    logging.error(
                        "Transfer verification found at line %d before sync started; inconsistent log",
                        idx,
                    )
                    sys.exit(4)

                local_info = json.loads(" ".join(info[1:]))
                fname = local_info["name"]
                size = int(local_info["size"])

                if sync_request["verified_files"].get(fname, None) == size:
                    continue

                if fname not in sync_request["files"]:
                    logging.error("File %s verified but was not requested.", fname)
                    sys.exit(4)
                if sync_request["files"][fname] != size:
                    logging.error(
                        "Verified file size %d of %s is different than anticipated",
                        size,
                        fname,
                        sync_request["files"][fname],
                    )
                    sys.exit(4)
                try:
                    local_size = os.stat(os.path.join(dest_dir, fname)).st_size
                except OSError as oe:
                    logging.error("Unable to verify size of %s: %s", fname, str(oe))
                    sys.exit(4)
                if local_size != size:
                    logging.error(
                        "Local size of %d of %s does not match anticipated size %d.",
                        local_size,
                        fname,
                        size,
                    )
                    sys.exit(4)
                if fname in sync_request["xfer_files"]:
                    sync_request["files_to_transfer"] -= 1
                    sync_request["bytes_to_transfer"] -= size
                else:
                    sync_request["files_to_verify"] -= 1
                    sync_request["bytes_to_verify"] -= size
                del sync_request["files"][fname]
                sync_request["verified_files"][fname] = size
            elif info[0] == "SYNC_DONE":
                if sync_request_start is None:
                    logging.error(
                        "Transfer request found at line %d before sync started; inconsistent log",
                        idx,
                    )
                    sys.exit(4)

                if (
                    sync_request["files_to_verify"]
                    or sync_request["bytes_to_verify"]
                    or sync_request["files"]
                    or sync_request["files_to_transfer"]
                    or sync_request["bytes_to_transfer"]
                ):
                    logging.error(
                        "SYNC_DONE but there is work remaining: %s", str(sync_request)
                    )
                    sys.exit(4)

                sync_request_start = None
                sync_request = {"files": {}, "xfer_files": set(), "verified_files": {}}

        if sync_request_start is not None and (
            sync_request["files_to_verify"]
            or sync_request["bytes_to_verify"]
            or sync_request["files"]
            or sync_request["files_to_transfer"]
            or sync_request["bytes_to_transfer"]
        ):
            logging.error("Sync not done! Work remaining.")
            logging.error(
                "- Files to transfer: %s (bytes %d)",
                sync_request["files_to_transfer"],
                sync_request["bytes_to_transfer"],
            )
            logging.error(
                "- Files to verify: %s (bytes %d)",
                sync_request["files_to_verify"],
                sync_request["bytes_to_verify"],
            )
            logging.error("Inconsistent files: {}".format(str(sync_request["files"])))
            sys.exit(4)

    if sync_request_start is not None:
        with transfer_manifest.open(mode="a") as f:
            f.write(f"SYNC_DONE {int(time.time())}\n")
        print("Synchronization done; verification complete.")
    elif sync_count:
        print("All synchronizations done; verification complete")
    else:
        logging.error("No synchronization found in manifest.")
        sys.exit(1)


def main():
    out = Path("/home/jtk/projects/htcondor_file_transfer") / "xfer.out"
    with out.open(mode="a") as f, contextlib.redirect_stdout(
        f
    ), contextlib.redirect_stderr(f):
        try:
            logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

            args = parse_args()

            print(f"Called with args: {args}")

            if args.cmd == "sync":
                if args.unique_id:
                    schedd = htcondor.Schedd()
                    existing_job = schedd.query(
                        constraint=f"UniqueId == {classad.quote(args.unique_id)} && JobStatus =!= 4",
                        attr_list=[],
                        limit=1,
                    )
                    if len(existing_job) > 0:
                        logging.warning(
                            'Jobs already found in queue with UniqueId == "%s", exiting',
                            args.unique_id,
                        )
                        sys.exit()
                working_dir = args.working_dir or os.getcwd()
                print(
                    f"Will synchronize {args.src} at source to {args.dest} at destination"
                )
                cluster_id = submit_parent_dag(
                    working_dir,
                    args.src,
                    os.path.abspath(args.dest),
                    requirements=read_requirements_file(args.requirements_file)
                    or args.requirements,
                    test_mode=args.test_mode,
                    unique_id=args.unique_id,
                )
                print(f"Parent job running in cluster {cluster_id}")
            elif args.cmd == "generate":
                logging.info("Generating file listing for %s", args.src)
                generate_file_listing(
                    args.src, "source_manifest.txt", test_mode=args.test_mode
                )
            elif args.cmd == "write_subdag":
                logging.info(
                    "Generating SUBGDAG for transfer of %s->%s",
                    args.source_prefix,
                    args.dest_prefix,
                )
                write_subdag(
                    args.source_prefix,
                    args.source_manifest,
                    args.dest_prefix,
                    args.dest_manifest,
                    args.transfer_manifest,
                    requirements=read_requirements_file(args.requirements_file)
                    or args.requirements,
                    test_mode=args.test_mode,
                    unique_id=args.unique_id,
                )
            elif args.cmd == "exec":
                xfer_exec(args.src)
            elif args.cmd == "verify":
                with open(args.json, "r") as fp:
                    cmd_info = json.load(fp)
                # Split the DAG job name to get the cmd_info key
                info = cmd_info[args.fileid.split(":")[-1]]
                verify(
                    info["dest_prefix"],
                    info["dest"],
                    f"{info['src_file_noslash']}.metadata",
                    info["transfer_manifest"],
                )
            elif args.cmd == "verify_remote":
                verify_remote(args.src)
            elif args.cmd == "analyze":
                analyze(args.transfer_manifest)
        except Exception:
            logging.exception("woops")


if __name__ == "__main__":
    main()
