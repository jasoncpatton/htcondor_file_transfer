#!/usr/bin/env python3

"""
Utilize HTCondor to transfer / synchronize a directory from a source on an
execute host to a local destination on the submit host.
"""

import argparse
import contextlib
import enum
import hashlib
import logging
import os
import shutil
import sys
import json
import time
from pathlib import Path
from typing import (
    Optional,
    Mapping,
    TypeVar,
    Dict,
    List,
    Any,
    Tuple,
    Iterator,
    Set,
    Iterable,
)

import htcondor
import classad

K = TypeVar("K")
V = TypeVar("V")
T_JSON = Dict[str, Any]
T_CMD_INFO = List[Mapping[str, Path]]

KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30
TB = 2 ** 40

METADATA_FILE_SIZE_LIMIT = 16 * KB
SANDBOX_FILE_NAME = "file-for-xfer"
REQUIREMENTS_FILE_NAME = "requirements.txt"
METADATA_FILE_NAME = "metadata"

THIS_FILE = Path(__file__).resolve()


class TransferError(Exception):
    pass


class InvalidMetadata(TransferError, ValueError):
    pass


class InconsistentManifest(TransferError, ValueError):
    pass


class TransferAlreadyRunning(TransferError):
    pass


class VerificationFailed(TransferError):
    pass


class NotACondorJob(TransferError):
    pass


def timestamp() -> float:
    return time.time()


def write_requirements_file(working_dir: Path, requirements: str) -> None:
    (working_dir / REQUIREMENTS_FILE_NAME).write_text(requirements)


def read_requirements_file(requirements_file: Optional[Path]) -> Optional[str]:
    if requirements_file is None:
        return None

    return requirements_file.read_text().strip()


class ManifestEntryType(str, enum.Enum):
    TRANSFER_REQUEST = "TRANSFER_REQUEST"
    VERIFY_REQUEST = "VERIFY_REQUEST"
    TRANSFER_VERIFIED = "TRANSFER_VERIFIED"
    SYNC_REQUEST = "SYNC_REQUEST"
    SYNC_DONE = "SYNC_DONE"
    FILE = "FILE"


def format_manifest_entry(type: ManifestEntryType, info: T_JSON) -> str:
    return "{} {}\n".format(type, json.dumps(path_values_to_strings(info)))


def read_manifest(path: Path) -> Iterator[Tuple[Tuple[ManifestEntryType, T_JSON], int]]:
    with path.open(mode="r") as f:
        for line, entry in enumerate(f, start=1):
            entry = entry.strip()

            if not entry or entry.startswith("#"):
                continue

            yield parse_manifest_entry(entry), line


def parse_manifest_entry(entry: str,) -> Tuple[ManifestEntryType, T_JSON]:
    entry = entry.strip()
    type, info = entry.split(maxsplit=1)

    type = ManifestEntryType(type)
    info = json.loads(info)

    if "size" in info:
        info["size"] = int(info["size"])

    return type, info


def create_file_manifest(
    root_path: Path, manifest_path: Path, test_mode: bool = False
) -> Path:
    with manifest_path.open(mode="w") as f:
        for entry in walk(root_path):
            size = entry.stat().st_size

            if test_mode and size > 50 * MB:
                continue

            info = {"name": entry.path, "size": size}
            f.write(format_manifest_entry(ManifestEntryType.FILE, info))

    return manifest_path


def parse_file_manifest(
    prefix: Path, file_manifest_path: Path, log_name: str
) -> Dict[str, int]:
    prefix = str(prefix.resolve())
    files = {}
    for (type, info), _ in read_manifest(file_manifest_path):
        if "name" not in info:
            raise Exception("File manifest entry missing 'name' key.  Info: %s" % info)
        fname = info["name"]
        if "size" not in info:
            raise Exception("File manifest entry missing 'size' key.  Info: %s" % info)
        size = info["size"]

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


def walk(path):
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
        "My.UniqueID": "{}".format(
            classad.quote(unique_id) if unique_id is not None else ""
        ),
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
        write_requirements_file(working_dir, requirements)

    outer_dag_file = dags.write_dag(
        outer_dag, dag_dir=working_dir, dag_file_name="outer.dag"
    )

    dag_args = {"force": 1}
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
                "arguments": "generate {} {}".format(
                    source_dir, "--test-mode" if test_mode else ""
                ),
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
                "--requirements_file={}".format(REQUIREMENTS_FILE_NAME)
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

    src_files = parse_file_manifest(source_prefix, source_manifest, "Source")

    create_file_manifest(dest_prefix, Path("destination_manifest.txt"))
    dest_files = parse_file_manifest(
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
    for (type, info), _ in read_manifest(transfer_manifest_path):
        if type is not ManifestEntryType.TRANSFER_VERIFIED:
            continue

        if not metadata_keys_present(info):
            continue

        files_verified.add(info["name"])

    files_to_verify = set()
    for fname in src_files:
        if fname in files_to_xfer:
            continue

        if fname not in files_verified:
            files_to_verify.add(fname)

    files_to_xfer = sorted(files_to_xfer)
    files_to_verify = sorted(files_to_verify)

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
            format_manifest_entry(
                ManifestEntryType.SYNC_REQUEST,
                {
                    "source_prefix": source_prefix,
                    "files_at_source": len(src_files),
                    "files_to_transfer": len(files_to_xfer),
                    "bytes_to_transfer": bytes_to_transfer,
                    "files_to_verify": len(files_to_verify),
                    "bytes_to_verify": bytes_to_verify,
                    "timestamp": timestamp(),
                },
            )
        )

        for fname in files_to_xfer:
            info = {"name": fname, "size": src_files[fname]}
            f.write(format_manifest_entry(ManifestEntryType.TRANSFER_REQUEST, info))
        for fname in files_to_verify:
            info = {"name": fname, "size": src_files[fname]}
            f.write(format_manifest_entry(ManifestEntryType.VERIFY_REQUEST, info))


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
                    "{} = $(dest); metadata = $(src_file_noslash).metadata".format(
                        SANDBOX_FILE_NAME
                    )
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
                "transfer_output_files": METADATA_FILE_NAME,
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


@contextlib.contextmanager
def change_dir(dir):
    original = os.getcwd()
    os.chdir(dir)
    yield
    os.chdir(original)


def ensure_destination_dirs_exist(prefix: Path, files_to_xfer: Iterable[str]):
    dest_dirs = {(prefix / relative_path).parent for relative_path in files_to_xfer}
    for dest_dir in dest_dirs:
        dest_dir.mkdir(exist_ok=True, parents=True)


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


def write_cmd_info(cmd_info: T_CMD_INFO, path: Path) -> None:
    with path.open("w") as cmd_fp:
        json.dump(dict(enumerate(map(path_values_to_strings, cmd_info))), cmd_fp)


def flatten_path(path: Path) -> str:
    return str(path).replace("/", "_SLASH_").replace(" ", "_SPACE_")


def path_values_to_strings(mapping):
    return {k: str(v) if isinstance(v, Path) else v for k, v in mapping.items()}


def xfer_exec(src_path: Path) -> None:
    check_running_as_job()

    dest_path = Path(os.environ["_CONDOR_SCRATCH_DIR"]) / SANDBOX_FILE_NAME

    hash, byte_count = copy_with_hash(src_path, dest_path)

    write_metadata_file(src_path, hash, byte_count)


def verify_remote(src_path: Path) -> None:
    check_running_as_job()

    hash, byte_count = hash_file(src_path)

    write_metadata_file(src_path, hash, byte_count)


def check_running_as_job():
    if "_CONDOR_JOB_AD" not in os.environ:
        raise NotACondorJob("This step must be run as an HTCondor job.")


def verify(
    dest_prefix: Path, dest: Path, metadata_path: Path, transfer_manifest_path: Path
) -> None:
    info = read_metadata_file(metadata_path)

    src_fname = info["name"]
    src_hexdigest = info["digest"]
    src_size = info["size"]

    relative_fname = dest.relative_to(dest_prefix)

    logging.info("About to verify contents of %s", dest)

    dest_size = dest.stat().st_size

    if src_size != dest_size:
        raise VerificationFailed(
            "Copied file size ({} bytes) does not match source file size ({} bytes)".format(
                dest_size, src_size,
            )
        )

    hash_obj, byte_count = hash_file(dest)

    dest_hexdigest = hash_obj.hexdigest()
    if src_hexdigest != dest_hexdigest:
        raise VerificationFailed(
            "Destination file {} has digest of {}, which does not match source file {} (digest {})".format(
                dest, dest_hexdigest, src_fname, src_hexdigest
            )
        )

    logging.info(
        "File verification successful: Destination (%s) and source (%s) have matching digest (%s)",
        dest,
        src_fname,
        src_hexdigest,
    )

    with transfer_manifest_path.open(mode="a") as f:
        info = {
            "name": str(relative_fname),
            "digest": src_hexdigest,
            "size": src_size,
            "timestamp": timestamp(),
        }
        f.write(format_manifest_entry(ManifestEntryType.TRANSFER_VERIFIED, info))

        os.fsync(f.fileno())

    metadata_path.unlink()
    if metadata_path.suffix == ".metadata":
        out_file = metadata_path.with_suffix(".out")
        if out_file.exists():
            out_file.unlink()
        err_file = metadata_path.with_suffix(".err")
        if err_file.exists():
            err_file.unlink()


def copy_with_hash(src_path: Path, dest_path: Path):
    tmp_path = dest_path.with_suffix(".tmp")
    logging.info("About to copy %s to %s", src_path, tmp_path)

    size = src_path.stat().st_size

    logging.info("There are %.2f MB to copy", size / MB)
    last_log = time.time()

    hash = hashlib.sha1()

    with src_path.open(mode="rb") as src, tmp_path.open(mode="wb") as dst:
        buf = src.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hash.update(buf)
            dst.write(buf)

            buf = src.read(MB)

            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Copied %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    size / MB,
                    (byte_count / size) * 100,
                )
                last_log = now

            byte_count += len(buf)

        logging.info("Copy complete; about to synchronize file to disk")

        os.fsync(dst.fileno())

        logging.info("File synchronized to disk")

        logging.info("Copying file metadata from {} to {}".format(src_path, tmp_path))

        shutil.copystat(src_path, tmp_path)

        logging.info("Copied file metadata")

    logging.info("Renaming {} to {}".format(tmp_path, dest_path))

    tmp_path.rename(dest_path)

    logging.info("Renamed {} to {}".format(tmp_path, dest_path))

    return hash, byte_count


def hash_file(path: Path):
    logging.info("About to hash %s", path)

    size = path.stat().st_size

    logging.info("There are %.2f MB to hash", size / MB)
    last_log = time.time()

    hash = hashlib.sha1()

    with path.open(mode="rb") as dest_fd:
        buf = dest_fd.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hash.update(buf)
            buf = dest_fd.read(MB)

            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Hashed %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    size / MB,
                    (byte_count / size) * 100,
                )
                last_log = now

            byte_count += len(buf)

    return hash, byte_count


def write_metadata_file(src_path: Path, hash, size: int) -> None:
    info = {
        "name": str(src_path),
        "digest": hash.hexdigest(),
        "size": size,
    }
    logging.info("File metadata: {}".format(info))

    write_json(Path(METADATA_FILE_NAME), info)

    logging.info("Wrote metadata file")


def read_metadata_file(path: Path) -> T_JSON:
    if path.stat().st_size > METADATA_FILE_SIZE_LIMIT:
        raise InvalidMetadata("Metadata file is too large")

    try:
        info = load_json(path)
    except json.JSONDecodeError as e:
        raise InvalidMetadata("Failed to load metadata from {}".format(path)) from e

    if not metadata_keys_present(info):
        raise InvalidMetadata("Metadata file is missing keys")

    info["size"] = int(info["size"])

    return info


def metadata_keys_present(metadata: T_JSON) -> bool:
    return all(key in metadata for key in {"name", "digest", "size"})


def analyze(transfer_manifest_path: Path) -> None:
    sync_request_start = None
    sync_request = {"files": {}, "xfer_files": set(), "verified_files": {}}
    dest_dir = transfer_manifest_path.parent.resolve()
    sync_count = 0

    for (type, info), line_number in read_manifest(transfer_manifest_path):
        # Format: SYNC_REQUEST {} files_at_source={} files_to_transfer={} bytes_to_transfer={} files_to_verify={} bytes_to_verify={} timestamp={}
        if type is ManifestEntryType.SYNC_REQUEST:
            sync_count += 1
            # if sync_request_start is not None:
            #    logging.error("Sync request started at line %d but never finished; inconsistent log",
            #        sync_request_start)
            #    sys.exit(4)
            sync_request_start = line_number
            sync_request.update(info)
        # Format: TRANSFER_REQUEST fname size
        elif type in (
            ManifestEntryType.TRANSFER_REQUEST,
            ManifestEntryType.VERIFY_REQUEST,
        ):
            if sync_request_start is None:
                raise InconsistentManifest(
                    "Transfer request found at line {} before sync started; inconsistent log".format(
                        line_number
                    )
                )

            size = info["size"]
            fname = info["name"]

            # File was previously verified.
            if sync_request["verified_files"].get(fname, None) == size:
                continue

            sync_request["files"][fname] = size

            if type is ManifestEntryType.TRANSFER_REQUEST:
                sync_request["xfer_files"].add(info["name"])
        # Format: TRANSFER_VERIFIED relative_fname hexdigest size timestamp:
        elif type is ManifestEntryType.TRANSFER_VERIFIED:
            if sync_request_start is None:
                raise InconsistentManifest(
                    "Transfer verification found at line {} before sync started; inconsistent log".format(
                        line_number
                    )
                )

            fname = info["name"]
            size = info["size"]

            if sync_request["verified_files"].get(fname, None) == size:
                continue

            if fname not in sync_request["files"]:
                raise InconsistentManifest(
                    "File {} verified but was not requested.".format(fname)
                )

            if sync_request["files"][fname] != size:
                raise InconsistentManifest(
                    "Verified file size {} of {} is different than anticipated {}".format(
                        size, fname, sync_request["files"][fname]
                    ),
                )

            local_size = (dest_dir / fname).stat().st_size
            if local_size != size:
                raise InconsistentManifest(
                    "Local size of {} of {} does not match anticipated size {}.".format(
                        local_size, fname, size,
                    )
                )

            if fname in sync_request["xfer_files"]:
                sync_request["files_to_transfer"] -= 1
                sync_request["bytes_to_transfer"] -= size
            else:
                sync_request["files_to_verify"] -= 1
                sync_request["bytes_to_verify"] -= size

            del sync_request["files"][fname]

            sync_request["verified_files"][fname] = size
        elif type is ManifestEntryType.SYNC_DONE:
            if sync_request_start is None:
                raise InconsistentManifest(
                    "Transfer request found at line {} before sync started; inconsistent log".format(
                        line_number,
                    )
                )

            if (
                sync_request["files_to_verify"]
                or sync_request["bytes_to_verify"]
                or sync_request["files"]
                or sync_request["files_to_transfer"]
                or sync_request["bytes_to_transfer"]
            ):
                raise InconsistentManifest(
                    "SYNC_DONE but there is work remaining: {}".format(sync_request)
                )

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
        logging.error("Inconsistent files: {}".format(sync_request["files"]))
        raise InconsistentManifest("There was work remaining!")

    if sync_request_start is not None:
        with transfer_manifest_path.open(mode="a") as f:
            f.write(
                format_manifest_entry(
                    ManifestEntryType.SYNC_DONE, {"timestamp": timestamp()}
                )
            )
        print("Synchronization done; verification complete.")
    elif sync_count:
        print("All synchronizations done; verification complete")
    else:
        raise InconsistentManifest("No synchronization found in manifest.")


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
    args = parse_args()

    print("Called with args: {}".format(args))

    if args.cmd == "sync":
        if args.unique_id:
            schedd = htcondor.Schedd()
            existing_job = schedd.query(
                constraint="UniqueId == {} && JobStatus =!= 4".format(
                    classad.quote(args.unique_id)
                ),
                attr_list=[],
                limit=1,
            )
            if len(existing_job) > 0:
                raise TransferAlreadyRunning(
                    'Jobs already found in queue with UniqueId == "{}"'.format(
                        args.unique_id,
                    )
                )
        print(
            "Will synchronize {} at source to {} at destination".format(
                args.src, args.dest
            )
        )
        cluster_id = submit_outer_dag(
            args.working_dir,
            args.src,
            args.dest,
            requirements=read_requirements_file(args.requirements_file)
            or args.requirements,
            unique_id=args.unique_id,
            test_mode=args.test_mode,
        )
        print("Outer DAG running in cluster {}".format(cluster_id))
    elif args.cmd == "generate":
        logging.info("Generating file listing for %s", args.src)
        create_file_manifest(
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
        cmd_info = load_json(args.json)
        # Split the DAG job name to get the cmd_info key
        info = cmd_info[args.fileid.split(":")[-1]]
        verify(
            Path(info["dest_prefix"]),
            Path(info["dest"]),
            Path("{}.metadata".format(info["src_file_noslash"])),
            Path(info["transfer_manifest"]),
        )
    elif args.cmd == "verify_remote":
        verify_remote(args.src)
    elif args.cmd == "analyze":
        analyze(args.transfer_manifest)


def write_json(path: Path, j: T_JSON) -> None:
    with path.open(mode="w") as f:
        json.dump(j, f)


def load_json(path: Path) -> T_JSON:
    with path.open(mode="r") as f:
        return json.load(f)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s ~ %(message)s", level=logging.INFO)

    try:
        main()
    except Exception as e:
        logging.exception("Error: {}".format(e))
        sys.exit(1)
