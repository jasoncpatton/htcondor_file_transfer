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
from typing import Iterator, Optional, Mapping, TypeVar, Dict, List

import htcondor
import classad


KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30
TB = 2 ** 40

METADATA_FILE_SIZE_LIMIT = 16 * KB
SANDBOX_FILE_NAME = "file-for-xfer"

THIS_FILE = Path(__file__).resolve()

K = TypeVar("K")
V = TypeVar("V")


def read_requirements_file(requirements_file: Optional[Path]) -> Optional[str]:
    if requirements_file is None:
        return None

    return requirements_file.read_text().strip()


def generate_file_listing(src: Path, manifest_path: Path, test_mode: bool = False):
    with manifest_path.open(mode="w") as f:
        for entry in walk(src):
            size = entry.stat().st_size

            if test_mode and size > 50 * MB:
                continue

            info = {"name": entry.path, "size": size}
            f.write("{}\n".format(json.dumps(info)))


def walk(path: Path):
    for entry in os.scandir(str(path)):
        if entry.is_dir():
            yield from walk(entry.path)
        elif entry.is_file():
            yield entry


def shared_submit_descriptors(unique_id=None, requirements=None):
    return {
        "executable": THIS_FILE.as_posix(),
        "My.Is_Transfer_Job": "true",
        "My.WantFlocking": "true",
        "keep_claim_idle": "300",
        "request_disk": "1GB",
        "requirements": requirements if requirements is not None else "true",
        "My.UniqueID": "{}".format(classad.quote(unique_id) if unique_id is not None else ''),
    }


def submit_outer_dag(
    working_dir: Path,
    source_dir: Path,
    dest_dir: Path,
    requirements: Optional[str] = None,
    unique_id: Optional[str] = None,
    test_mode: bool = False,
):

    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    working_dir = working_dir.resolve()
    dest_dir = dest_dir.resolve()

    working_dir.mkdir(parents=True, exist_ok=True)
    dest_dir.mkdir(parents=True, exist_ok=True)

    transfer_manifest_path = dest_dir / "transfer_manifest.txt"

    outer_dag = make_outer_dag(
        dest_dir,
        requirements,
        source_dir,
        test_mode,
        transfer_manifest_path,
        unique_id,
        working_dir,
    )

    if requirements:
        (working_dir / "requirements.txt").write_text(requirements)

    outer_dag_file = dags.write_dag(
        outer_dag, dag_dir=working_dir, dag_file_name="outer.dag"
    )

    dag_args = {'force': 1}
    sub = htcondor.Submit.from_dag(str(outer_dag_file), dag_args)

    with change_dir(working_dir):
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            return sub.queue(txn)


def make_outer_dag(
    dest_dir,
    requirements,
    source_dir,
    test_mode,
    transfer_manifest_path,
    unique_id,
    working_dir,
):

    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    outer_dag = dags.DAG()

    outer_dag.layer(
        name="calc_work",
        submit_description=htcondor.Submit(
            {
                "output": "calc_work.out",
                "error": "calc_work.err",
                "log": "calc_work.log",
                "arguments": "generate {} {}".format(source_dir, '--test-mode' if test_mode else ''),
                "should_transfer_files": "yes",
                **shared_submit_descriptors(unique_id, requirements),
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

    return outer_dag


@contextlib.contextmanager
def change_dir(dir):
    original = os.getcwd()
    os.chdir(dir)
    yield
    os.chdir(original)


def parse_manifest(prefix: Path, manifest_path: Path, log_name):
    prefix = str(prefix.resolve())
    files = {}
    with manifest_path.open(mode="r") as fd:
        for line in fd:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

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


def write_inner_dag(
    source_prefix: Path,
    source_manifest: Path,
    dest_prefix: Path,
    requirements=None,
    test_mode: bool = False,
    unique_id=None,
):

    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    src_files = parse_manifest(source_prefix, source_manifest, "Source")

    generate_file_listing(dest_prefix, Path("destination_manifest.txt"))
    dest_files = parse_manifest(
        dest_prefix, Path("destination_manifest.txt"), "Destination"
    )

    files_to_xfer = set()
    for fname in src_files:
        if src_files[fname] != dest_files.get(fname, -1):
            files_to_xfer.add(fname)

    transfer_manifest_path = Path(os.path.join(dest_prefix, "transfer_manifest.txt"))
    transfer_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    transfer_manifest_path.touch(exist_ok=True)

    # Check for files that we have already verified, and do not verify them again.
    files_verified = set()
    with transfer_manifest_path.open(mode="r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            info = line.split()

            if info[0] != "TRANSFER_VERIFIED":
                continue

            info = json.loads(" ".join(info[1:]))
            if not valid_metadata(info):
                continue

            files_verified.add(info["name"])

    files_to_verify = set()
    for fname in src_files:
        if fname in files_to_xfer:
            continue

        if fname not in files_verified:
            files_to_verify.add(fname)

    ensure_destination_dirs_exist(dest_prefix, files_to_xfer)

    xfer_cmd_info = make_cmd_info(
        files_to_xfer, source_prefix, dest_prefix, transfer_manifest_path
    )
    verify_cmd_info = make_cmd_info(
        files_to_verify, source_prefix, dest_prefix, transfer_manifest_path
    )

    write_cmd_info(xfer_cmd_info, Path("xfer_commands.json"))
    write_cmd_info(verify_cmd_info, Path("verify_commands.json"))

    inner_dag = make_inner_dag(
        requirements, xfer_cmd_info, verify_cmd_info, unique_id, test_mode
    )

    print(inner_dag.describe())

    dags.write_dag(inner_dag, dag_dir=Path.cwd(), dag_file_name="inner.dag")

    bytes_to_transfer = sum(src_files[fname] for fname in files_to_xfer)
    bytes_to_verify = sum(src_files[fname] for fname in files_to_verify)
    with transfer_manifest_path.open(mode="a") as f:
        f.write(
            "SYNC_REQUEST {} files_at_source={} files_to_transfer={} bytes_to_transfer={} files_to_verify={} bytes_to_verify={} timestamp={}\n".format(source_prefix, len(src_files), len(files_to_xfer), bytes_to_transfer, len(files_to_verify), bytes_to_verify, time.time())
        )

        for fname in files_to_xfer:
            info = {"name": fname, "size": src_files[fname]}
            f.write("TRANSFER_REQUEST {}\n".format(json.dumps(info)))
        for fname in files_to_verify:
            info = {"name": fname, "size": src_files[fname]}
            f.write("VERIFY_REQUEST {}\n".format(json.dumps(info)))


def ensure_destination_dirs_exist(dest_prefix, files_to_xfer):
    dest_dirs = set()
    for idx, fname in enumerate(sorted(files_to_xfer)):
        dest = os.path.join(dest_prefix, fname)
        dest_dirs.add(os.path.split(dest)[0])

    for dest_dir in dest_dirs:
        Path(dest_dir).mkdir(exist_ok=True, parents=True)


T_CMD_INFO = List[Mapping[str, Path]]


def make_cmd_info(files, source_prefix, dest_prefix, transfer_manifest_path):
    cmd_info = []

    for fname in files:
        src_file = os.path.join(source_prefix, fname)
        src_file_noslash = flatten_path(fname)
        dest = os.path.join(dest_prefix, fname)

        info = {
            "src_file": src_file,
            "src_file_noslash": src_file_noslash,
            "dest": dest,
            "transfer_manifest": transfer_manifest_path,
            "dest_prefix": dest_prefix,
        }
        cmd_info.append(info)

    return cmd_info


def write_cmd_info(cmd_info: T_CMD_INFO, path: Path):
    with path.open("w") as cmd_fp:
        json.dump(dict(enumerate(map(values_to_strings, cmd_info))), cmd_fp)


def flatten_path(path: Path) -> str:
    return str(path).replace("/", "_SLASH_").replace(" ", "_SPACE_")


def values_to_strings(mapping: Mapping[K, V]) -> Dict[K, str]:
    return {k: str(v) for k, v in mapping.items()}


def make_inner_dag(
    requirements: Optional[str],
    xfer_cmd_info: T_CMD_INFO,
    verify_cmd_info: T_CMD_INFO,
    unique_id: Optional[str] = None,
    test_mode: bool = False,
):

    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    inner_dag = dags.DAG(
        max_jobs_by_category={"TRANSFER_JOBS": 1} if test_mode else None
    )

    inner_dag.layer(
        name="xfer",
        submit_description=htcondor.Submit(
            {
                "output": "$(src_file_noslash).out",
                "error": "$(src_file_noslash).err",
                "log": "xfer_file.log",
                "arguments": classad.quote("exec '$(src_file)'"),
                "should_transfer_files": "yes",
                "transfer_output_files": "{}, metadata".format(SANDBOX_FILE_NAME),
                "transfer_output_remaps": classad.quote(
                    "{} = $(dest); metadata = $(src_file_noslash).metadata".format(SANDBOX_FILE_NAME)
                ),
                **shared_submit_descriptors(unique_id, requirements),
            }
        ),
        vars=xfer_cmd_info,
        post=dags.Script(
            executable=THIS_FILE,
            arguments=["verify", "--json=xfer_commands.json", "--fileid", "$JOB"],
        ),
    )

    inner_dag.layer(
        name="verify",
        submit_description=htcondor.Submit(
            {
                "output": "$(src_file_noslash).out",
                "error": "$(src_file_noslash).err",
                "log": "verify_file.log",
                "arguments": classad.quote("verify_remote '$(src_file)'"),
                "should_transfer_files": "yes",
                "transfer_output_files": "metadata",
                "transfer_output_remaps": classad.quote(
                    "metadata = $(src_file_noslash).metadata"
                ),
                **shared_submit_descriptors(unique_id, requirements),
            }
        ),
        vars=verify_cmd_info,
        post=dags.Script(
            executable=THIS_FILE,
            arguments=["verify", "--json=verify_commands.json", "--fileid", "$JOB"],
        ),
    )

    return inner_dag


def xfer_exec(src_path: Path):
    if "_CONDOR_JOB_AD" not in os.environ:
        print("This executable must be run within the HTCondor runtime environment.")
        sys.exit(1)

    dest_path = Path(os.environ["_CONDOR_SCRATCH_DIR"]) / SANDBOX_FILE_NAME
    tmp_path = dest_path.with_suffix(".tmp")

    logging.info("About to copy %s to %s", src_path, tmp_path)

    file_size = src_path.stat().st_size
    logging.info("There are %.2f MB to copy", file_size / MB)
    last_log = time.time()

    hash_obj = hashlib.sha1()

    with src_path.open(mode="rb") as src, tmp_path.open(mode="wb") as dst:
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

    logging.info("Renaming {} to {}".format(tmp_path, dest_path))

    tmp_path.rename(dest_path)

    logging.info("Renamed {} to {}".format(tmp_path, dest_path))

    info = {
        "name": str(src_path),
        "digest": hash_obj.hexdigest(),
        "size": byte_count,
    }
    logging.info("File metadata: {}".format(info))

    Path("metadata").write_text("{}\n".format(json.dumps(info)))

    logging.info("Wrote metadata file")


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

    with Path("metadata").open(mode="w") as metadata:
        info = {"name": str(src), "digest": hash_obj.hexdigest(), "size": byte_count}
        metadata.write("{}\n".format(json.dumps(info)))


def verify(dest_prefix: Path, dest: Path, metadata_path: Path, metadata_summary: Path):
    if metadata_path.stat().st_size > 16384:
        logging.error("Metadata file is too large")
        sys.exit(1)

    contents = metadata_path.read_text().strip()

    if not contents:
        logging.error("Metadata file is empty")
        sys.exit(1)

    info = json.loads(contents)

    if not valid_metadata(info):
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
        md_fd.write("TRANSFER_VERIFIED {}\n".format(json.dumps(info)))
        os.fsync(md_fd.fileno())

    metadata_path.unlink()
    if metadata_path.suffix == ".metadata":
        out_file = metadata_path.with_suffix(".out")
        if out_file.exists():
            out_file.unlink()
        err_file = metadata_path.with_suffix(".err")
        if err_file.exists():
            err_file.unlink()


def valid_metadata(metadata) -> bool:
    return all(key in metadata for key in {"name", "digest", "size"})


def analyze(transfer_manifest: Path):
    sync_request_start = None
    sync_request = {"files": {}, "xfer_files": set(), "verified_files": {}}
    dest_dir = os.path.abspath(os.path.split(transfer_manifest)[0])
    sync_count = 0

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
                local_info = json.loads(" ".join(info[1:]))
                sync_request["xfer_files"].add(local_info["name"])
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
            f.write("SYNC_DONE {}\n".format(int(time.time())))
        print("Synchronization done; verification complete.")
    elif sync_count:
        print("All synchronizations done; verification complete")
    else:
        logging.error("No synchronization found in manifest.")
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    sync = subparsers.add_parser("sync")
    sync.add_argument("src", type=Path)
    sync.add_argument("dest", type=Path)
    default_working_dir = Path.cwd() / "xfer_working_dir"
    sync.add_argument(
        "--working-dir",
        help="Directory to place working HTCondor files.",
        type=Path,
        default=default_working_dir,
        dest="working_dir",
    )
    sync.add_argument(
        "--requirements",
        help="Submit file requirements (e.g. 'UniqueName == \"MyLab0001\"')",
    )
    sync.add_argument(
        "--requirements_file",
        help="File containing submit file requirements",
        type=Path,
    )
    sync.add_argument(
        "--unique-id",
        help="Do not submit if jobs with UniqueId already found in queue",
        dest="unique_id",
    )
    add_test_mode_arg(sync)

    generate = subparsers.add_parser("generate")
    generate.add_argument("src", type=Path)
    add_test_mode_arg(generate)

    subdag = subparsers.add_parser("write_subdag")
    subdag.add_argument("source_prefix", type=Path)
    subdag.add_argument("source_manifest", type=Path)
    subdag.add_argument("dest_prefix", type=Path)
    subdag.add_argument("dest_manifest", type=Path)
    subdag.add_argument("transfer_manifest", type=Path)
    subdag.add_argument("--requirements", help="Submit file requirements")
    subdag.add_argument(
        "--requirements_file",
        help="File containing submit file requirements",
        type=Path,
    )
    subdag.add_argument(
        "--unique-id", help="Set UniqueId in submitted jobs", dest="unique_id"
    )
    add_test_mode_arg(subdag)

    exec = subparsers.add_parser("exec")
    exec.add_argument("src", type=Path)

    verify_remote = subparsers.add_parser("verify_remote")
    verify_remote.add_argument("src", type=Path)

    verify = subparsers.add_parser("verify")
    # SCRIPT POST xfer_{name} {xfer_py} verify {dest_prefix} {dest} {src_file_noslash}.metadata {transfer_manifest}
    # verify.add_argument("dest_prefix")
    # verify.add_argument("dest")
    # verify.add_argument("metadata")
    # verify.add_argument("metadata_summary")
    verify.add_argument("--json", type=Path)
    verify.add_argument("--fileid")

    analyze = subparsers.add_parser("analyze")
    analyze.add_argument("transfer_manifest", type=Path)

    return parser.parse_args()


def add_test_mode_arg(parser):
    parser.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
        dest="test_mode",
    )


def main():
    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

    args = parse_args()

    print("Called with args: {}".format(args))

    if args.cmd == "sync":
        if args.unique_id:
            schedd = htcondor.Schedd()
            existing_job = schedd.query(
                constraint="UniqueId == {} && JobStatus =!= 4".format(classad.quote(args.unique_id)),
                attr_list=[],
                limit=1,
            )
            if len(existing_job) > 0:
                logging.warning(
                    'Jobs already found in queue with UniqueId == "%s", exiting',
                    args.unique_id,
                )
                sys.exit()
        print("Will synchronize {} at source to {} at destination".format(args.src, args.dest))
        cluster_id = submit_outer_dag(
            args.working_dir,
            args.src,
            args.dest,
            requirements=read_requirements_file(args.requirements_file)
            or args.requirements,
            unique_id=args.unique_id,
            test_mode=args.test_mode,
        )
        print("Parent job running in cluster {}".format(cluster_id))
    elif args.cmd == "generate":
        logging.info("Generating file listing for %s", args.src)
        generate_file_listing(
            args.src, Path("source_manifest.txt"), test_mode=args.test_mode
        )
    elif args.cmd == "write_subdag":
        logging.info(
            "Generating SUBGDAG for transfer of %s->%s",
            args.source_prefix,
            args.dest_prefix,
        )
        write_inner_dag(
            args.source_prefix,
            args.source_manifest,
            args.dest_prefix,
            requirements=read_requirements_file(args.requirements_file)
            or args.requirements,
            test_mode=args.test_mode,
            unique_id=args.unique_id,
        )
    elif args.cmd == "exec":
        xfer_exec(args.src)
    elif args.cmd == "verify":
        with args.json.open(mode="r") as f:
            cmd_info = json.load(f)
        # Split the DAG job name to get the cmd_info key
        info = cmd_info[args.fileid.split(":")[-1]]
        verify(
            Path(info["dest_prefix"]),
            Path(info["dest"]),
            Path("{}.metadata".format(info['src_file_noslash'])),
            Path(info["transfer_manifest"]),
        )
    elif args.cmd == "verify_remote":
        verify_remote(args.src)
    elif args.cmd == "analyze":
        analyze(args.transfer_manifest)


if __name__ == "__main__":
    main()
