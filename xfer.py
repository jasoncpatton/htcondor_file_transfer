#!/usr/bin/python

"""
Utilize HTCondor to transfer / synchronize a directory from a source on an
execute host to a local destination on the submit host.
"""

from __future__ import print_function, unicode_literals

import argparse
import errno
import hashlib
import logging
import os
import re
import sys
import json
import time

import htcondor


PARENT_DAG = \
"""
# Gather the remote file listing.
JOB CalculateWork calc_work.sub DIR calc_work

# Gather the local file listing and write out the sub-DAG
# to perform the actual transfers
SCRIPT POST CalculateWork {exec_py} write_subdag {source_prefix} source_manifest.txt {dest_prefix} destination_manifest.txt {transfer_manifest} {requirements} {unique_id} {other_args}

SUBDAG EXTERNAL DoXfers calc_work/do_work.dag
SCRIPT POST DoXfers {exec_py} analyze {transfer_manifest}

PARENT CalculateWork CHILD DoXfers
"""

CALC_WORK_JOB = \
"""
universe = vanilla
executable = {exec_py}
output = calc_work.out
error = calc_work.err
log = calc_work.log
arguments = generate {source_dir} {other_args}
should_transfer_files = YES
transfer_output_files = source_manifest.txt
requirements = {requirements}
+IS_TRANSFER_JOB = true
+UniqueId = {unique_id}
+WantFlocking = true
keep_claim_idle = 300

queue
"""

XFER_FILE_JOB = \
"""
universe = vanilla
executable = {xfer_py}
output = $(src_file_noslash).out
error = $(src_file_noslash).err
log = xfer_file.log
Args = "exec '$(src_file)'"
should_transfer_files = YES
transfer_output_files = file0, metadata
transfer_output_remaps = "file0 = $(dst_file); metadata = $(src_file_noslash).metadata"
requirements = {requirements}
+IS_TRANSFER_JOB = true
+UniqueId = {unique_id}
+WantFlocking = true

queue
"""

XFER_FILE_N_JOB = \
"""
universe = vanilla
executable = {xfer_py}
output = $(name).out
error = $(name).err
log = xfer_file.log
Args = "exec_n --json '$(name)'"
should_transfer_files = YES
transfer_output_files = {file_list}, metadata
transfer_output_remaps = "{file_list}; metadata = result_$(name).metadata"
requirements = {requirements}
+IS_TRANSFER_JOB = true
+UniqueId = {unique_id}
+WantFlocking = true

queue
"""

VERIFY_FILE_JOB = \
"""
universe = vanilla
executable = {xfer_py}
output = $(src_file_noslash).out
error = $(src_file_noslash).err
log = xfer_file.log
Args = "verify_remote '$(src_file)'"
should_transfer_files = YES
transfer_output_files = metadata
transfer_output_remaps = "metadata = $(src_file_noslash).metadata"
requirements = {requirements}
+IS_TRANSFER_JOB = true
+UniqueId = {unique_id}
+WantFlocking = true

queue
"""

DO_WORK_DAG_HEADER = \
"""
CATEGORY ALL_NODES TRANSFER_JOBS
{}

"""

DO_WORK_DAG_XFER_SNIPPET = \
"""
JOB xfer_{name} xfer_file.sub DIR calc_work
VARS xfer_{name} src_file_noslash="{src_file_noslash}"
VARS xfer_{name} src_file="{src_file}"
VARS xfer_{name} dst_file="{dest}"
SCRIPT POST xfer_{name} {xfer_py} verify --json=xfer_commands_{fileidx}.json --fileid={name}

"""

DO_WORK_DAG_VERIFY_SNIPPET = \
"""
JOB verify_{name} verify_file.sub DIR calc_work
VARS verify_{name} src_file_noslash="{src_file_noslash}"
VARS verify_{name} src_file="{src_file}"
VARS verify_{name} dst_file="{dest}"
SCRIPT POST verify_{name} {xfer_py} verify --json=verify_commands_{fileidx}.json --fileid={name}

"""


_SIMPLE_FNAME_RE = re.compile("^[0-9A-Za-z_./:\-]+$")
def simple_fname(fname):
    return _SIMPLE_FNAME_RE.match(fname)


def search_path(exec_name):
    for path in os.environ.get("PATH", "/usr/bin:/bin").split(":"):
        fname = os.path.join(path, exec_name)
        if os.access(fname, os.X_OK):
            return fname


def read_requirements_file(requirements_file):
    requirements = None
    if requirements_file is not None:
        with open(requirements_file, 'r') as f:
            requirements = f.readline().rstrip()
    return requirements


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    parser_sync = subparsers.add_parser("sync")
    parser_sync.add_argument("src")
    parser_sync.add_argument("dest")
    parser_sync.add_argument("--working-dir", help="Directory to place working HTCondor files.",
        default="./scratch_dir", dest="working_dir")
    parser_sync.add_argument("--requirements",
        help="Submit file requirements (e.g. 'UniqueName == \"MyLab0001\"')")
    parser_sync.add_argument("--requirements_file", help="File containing submit file requirements")
    parser_sync.add_argument("--unique-id", help="Do not submit if jobs with UniqueId already found in queue",
        dest="unique_id")
    parser_sync.add_argument("--test-mode", help="Testing mode (only transfers small files)",
        default=False, action="store_true", dest="test_mode")

    parser_generate = subparsers.add_parser("generate")
    parser_generate.add_argument("src")
    parser_generate.add_argument("--test-mode", help="Testing mode (only transfers small files)",
        default=False, action="store_true", dest="test_mode")

    parser_subdag = subparsers.add_parser("write_subdag")
    parser_subdag.add_argument("source_prefix")
    parser_subdag.add_argument("source_manifest")
    parser_subdag.add_argument("dest_prefix")
    parser_subdag.add_argument("dest_manifest")
    parser_subdag.add_argument("transfer_manifest")
    parser_subdag.add_argument("--requirements", help="Submit file requirements")
    parser_subdag.add_argument("--requirements_file", help="File containing submit file requirements")
    parser_subdag.add_argument("--unique-id", help="Set UniqueId in submitted jobs", dest="unique_id")
    parser_subdag.add_argument("--test-mode", help="Testing mode (only transfers small files)",
        default=False, action="store_true", dest="test_mode")

    parser_exec = subparsers.add_parser("exec")
    parser_exec.add_argument("src")

    parser_verify_remote = subparsers.add_parser("verify_remote")
    parser_verify_remote.add_argument("src")

    parser_verify = subparsers.add_parser("verify")
    # SCRIPT POST xfer_{name} {xfer_py} verify {dest_prefix} {dest} {src_file_noslash}.metadata {transfer_manifest}
    #parser_verify.add_argument("dest_prefix")
    #parser_verify.add_argument("dest")
    #parser_verify.add_argument("metadata")
    #parser_verify.add_argument("metadata_summary")
    parser_verify.add_argument("--json", dest="json")
    parser_verify.add_argument("--fileid", dest="fileid")

    parser_analyze = subparsers.add_parser("analyze")
    parser_analyze.add_argument("transfer_manifest")

    return parser.parse_args()


def generate_file_listing(src, manifest, test_mode=False):
    with open(manifest, "w") as fp:
        for root, dirs, fnames in os.walk(src):
            for fname in fnames:
                full_fname = os.path.normpath(os.path.join(root, fname))
                size = os.stat(full_fname).st_size
                if test_mode and size > 50*1024*1024:
                    continue
                if simple_fname(full_fname):
                    fp.write("{} {}\n".format(full_fname, size))
                else:
                    info = {'name': full_fname, 'size': size}
                    fp.write("{}\n".format(json.dumps(info)))


def submit_parent_dag(working_dir, source_dir, dest_dir, requirements=None, test_mode=False, unique_id=None):
    try:
        os.makedirs(os.path.join(working_dir, "calc_work"))
    except OSError as oe:
        if oe.errno != errno.EEXIST:
            raise

    info = os.path.split(sys.argv[0])
    full_exec_path = os.path.join(os.path.abspath(info[0]), info[1])

    if requirements is not None:
        with open(os.path.join(working_dir, "calc_work", "requirements.txt"), "w") as fd:
            fd.write(requirements)

    with open(os.path.join(working_dir, "xfer.dag"), "w") as fd:
        fd.write(PARENT_DAG.format(exec_py=full_exec_path, source_prefix=source_dir,
            dest_prefix=dest_dir, other_args="--test-mode" if test_mode else "",
            transfer_manifest=os.path.join(dest_dir, "transfer_manifest.txt"),
            requirements="--requirements_file=requirements.txt" if requirements is not None else "",
            unique_id="--unique_id={}".format(unique_id) if unique_id is not None else ""))

    with open(os.path.join(working_dir, "calc_work", "calc_work.sub"), "w") as fd:
        fd.write(CALC_WORK_JOB.format(exec_py=full_exec_path, source_dir=source_dir,
            other_args="--test-mode" if test_mode else "",
            requirements=requirements if requirements is not None else "True",
            unique_id=unique_id if unique_id is not None else ""))

    dagman = search_path("condor_dagman")
    if not dagman:
        print("Unable to find the `condor_dagman` executable in the $PATH")
        sys.exit(1)

    submit_dict = {
        b"universe":                    b"scheduler",
        b"executable":                  dagman.encode('utf-8'),
        b"output":                      b"xfer.dag.lib.out",
        b"error":                       b"xfer.dag.lib.err",
        b"log":                         b"xfer.dag.dagman.log",
        b"remove_kill_Sig":             b"SIGUSR1",
        b"+OtherJobRemoveRequirements": b'"DAGManJobId =?= $(cluster)"',
        b"on_exit_remove":              b'(ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && '
                                        b'ExitCode >=0 && ExitCode <= 2))',
        b"arguments":                   b"\"-p 0 -f -l . -Lockfile xfer.dag.lock -AutoRescue 1 -DoRescueFrom 0"
                                        b" -Dag xfer.dag -Suppress_notification -Dagman /usr/bin/condor_dagman"
                                        b" -CsdVersion {}\"".format(htcondor.version().replace(" ", "' '")),
        b"environment":                 b"_CONDOR_SCHEDD_ADDRESS_FILE={};_CONDOR_MAX_DAGMAN_LOG=0;"
                                        b"_CONDOR_SCHEDD_DAEMON_AD_FILE={};_CONDOR_DAGMAN_LOG=xfer.dag.dagman.out".format(
            htcondor.param[b'SCHEDD_ADDRESS_FILE'], htcondor.param[b'SCHEDD_DAEMON_AD_FILE'])
    }

    schedd = htcondor.Schedd()

    orig_pwd = os.getcwd()
    try:
        os.chdir(working_dir)
        sub = htcondor.Submit(submit_dict)
        with schedd.transaction() as txn:
            return sub.queue(txn)
    finally:
        os.chdir(orig_pwd)


def parse_manifest(prefix, manifest, log_name):

    prefix = os.path.normpath(prefix)
    files = {}
    with open(manifest, "r") as fd:
        for line in fd.readlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("{"):
                info = json.loads(line)
                if 'name' not in info:
                    raise Exception("Manifest line missing 'name' key.  Current line: %s" % line)
                fname = info['name']
                if 'size' not in info:
                    raise Exception("Manifest line missing 'size' key.  Currenty line: %s" % line)
                size = int(info['size'])
            else:
                info = line.strip().split()
                if len(info) != 2:
                    raise Exception("Manifest lines must have two columns.  Current line: %s" % line)
                fname = info[0].decode('utf-8')
                size = int(info[1])
            if not fname.startswith(prefix):
                logging.error("%s file (%s) does not start with specified prefix", log_name, fname)
            fname = fname[len(prefix) + 1:]
            if not fname:
                logging.warning("%s file, stripped of prefix (%s), is empty", log_name, prefix)
                continue
            files[fname] = size
    return files


def write_subdag(source_prefix, source_manifest, dest_prefix, dest_manifest, transfer_manifest, requirements=None, test_mode=False, unique_id=None):
    src_files = parse_manifest(source_prefix, source_manifest, "Source")

    generate_file_listing(dest_prefix, "destination_manifest.txt")
    dest_files = parse_manifest(dest_prefix, "destination_manifest.txt", "Destination")

    files_to_xfer = set()
    for fname in src_files:
        if src_files[fname] != dest_files.get(fname, -1):
            files_to_xfer.add(fname)

    transfer_manifest = os.path.join(dest_prefix, "transfer_manifest.txt")
    if not os.path.exists(transfer_manifest):
        try:
            os.makedirs(dest_prefix)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise
        fd = os.open(transfer_manifest, os.O_CREAT|os.O_RDONLY)
        os.close(fd)

    files_verified = set()
    with open(transfer_manifest, "r") as fp:
        for line in fp.readlines():
            line = line.strip()
            if not line or line[0] == '#':
                continue
            info = line.strip().split()
            if info[0] != 'TRANSFER_VERIFIED':
                continue
            if info[1] == '{':
                info = json.loads(" ".join(info[1:]))
                if 'name' not in info or 'digest' not in info or \
                        'size' not in info:
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

    info = os.path.split(sys.argv[0])
    full_exec_path = os.path.join(os.path.abspath(info[0]), info[1])

    with open("xfer_file.sub", "w") as fp:
        fp.write(XFER_FILE_JOB.format(xfer_py=full_exec_path,
            requirements=requirements if requirements is not None else "True",
            unique_id=unique_id if unique_id is not None else ""))
    with open("verify_file.sub", "w") as fp:
        fp.write(VERIFY_FILE_JOB.format(xfer_py=full_exec_path,
            requirements=requirements if requirements is not None else "True",
            unique_id=unique_id if unique_id is not None else ""))

    idx = 0
    dest_dirs = set()
    jsonidx = 0
    cur_jsonidx = 0
    cmd_info = {}
    with open("do_work.dag", "w") as fp:
        fp.write(DO_WORK_DAG_HEADER.format("MAXJOBS TRANSFER_JOBS 1" if test_mode else ""))
        files_to_xfer = list(files_to_xfer)
        files_to_xfer.sort()
        for fname in files_to_xfer:
            idx += 1
            src_file = os.path.join(source_prefix, fname)
            src_file_noslash = fname.replace("/", "_")
            dest = os.path.join(dest_prefix, fname)
            dest_dirs.add(os.path.split(dest)[0])

            logging.info("File transfer to perform: %s->%s", src_file, dest)
            cur_jsonidx = idx / 1000
            if jsonidx != cur_jsonidx:
                # xfer_commands_{fileidx}.json
                with open("xfer_commands_{}.json".format(jsonidx), "w") as cmd_fp:
                    json.dump(cmd_info, cmd_fp)
                jsonidx = cur_jsonidx
                cmd_info = {}
            cmd_info[str(idx)] = {
                "src_file": src_file,
                "src_file_noslash": src_file_noslash,
                "dest": dest,
                "transfer_manifest": transfer_manifest,
                "dest_prefix": dest_prefix
            }
            fp.write(DO_WORK_DAG_XFER_SNIPPET.format(name=idx, fileidx=cur_jsonidx,
                xfer_py=full_exec_path, src_file=src_file, src_file_noslash=src_file_noslash,
		dest=dest))

        with open("xfer_commands_{}.json".format(cur_jsonidx), "w") as cmd_fp:
            json.dump(cmd_info, cmd_fp)

        idx = 0
        jsonidx = 0
        cur_jsonidx = 0
        for fname in files_to_verify:
            idx += 1
            src_file = os.path.join(source_prefix, fname)
            src_file_noslash = fname.replace("/", "_")
            dest = os.path.join(dest_prefix, fname)
            logging.info("File to verify: %s", src_file)
            cur_jsonidx = idx / 1000
            if jsonidx != cur_jsonidx:
                with open("verify_commands_{}.json".format(jsonidx), "w") as cmd_fp:
                    json.dump(cmd_info, cmd_fp)
                jsonidx = cur_jsonidx
                cmd_info = {}
            cmd_info[str(idx)] = {
                "src_file": src_file,
                "src_file_noslash": src_file_noslash,
                "dest": dest,
                "transfer_manifest": transfer_manifest,
                "dest_prefix": dest_prefix
            }
            fp.write(DO_WORK_DAG_VERIFY_SNIPPET.format(name=idx, fileidx=cur_jsonidx,
                xfer_py=full_exec_path, src_file=src_file, src_file_noslash=src_file_noslash,
                dest=dest))

        with open("verify_commands_{}.json".format(cur_jsonidx), "w") as cmd_fp:
            json.dump(cmd_info, cmd_fp)

    for dest_dir in dest_dirs:
        try:
            os.makedirs(dest_dir)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise

    bytes_to_transfer = sum(src_files[fname] for fname in files_to_xfer)
    bytes_to_verify = sum(src_files[fname] for fname in files_to_verify)
    with open(transfer_manifest, "a") as fp:
        fp.write("SYNC_REQUEST {} files_at_source={} files_to_transfer={} bytes_to_transfer={} files_to_verify={} bytes_to_verify={} timestamp={}\n".format(
            source_prefix, len(src_files), len(files_to_xfer), bytes_to_transfer, len(files_to_verify), bytes_to_verify, time.time()))
        for fname in files_to_xfer:
            if simple_fname(fname):
                fp.write("TRANSFER_REQUEST {} {}\n".format(fname, src_files[fname]))
            else:
                info = {"name": fname, "size": src_files[fname]}
                fp.write("TRANSFER_REQUEST {}\n".format(json.dumps(info)))
        for fname in files_to_verify:
            if simple_fname(fname):
                fp.write("VERIFY_REQUEST {} {}\n".format(fname, src_files[fname]))
            else:
                info = {"name": fname, "size": src_files[fname]}
                fp.write("VERIFY_REQUEST {}\n".format(json.dumps(info)))


def xfer_exec(src):
    if '_CONDOR_JOB_AD' not in os.environ:
        print("This executable must be run within the HTCondor runtime environment.")
        sys.exit(1)

    logging.info("About to copy %s to file0", src)
    src_fd = open(src, "r")
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
        if simple_fname(src):
            metadata_fd.write("{} {} {}\n".format(src, hash_obj.hexdigest(), byte_count).encode('utf-8'))
        else:
            info = {"name": src, "digest": hash_obj.hexdigest(), "size": byte_count}
            metadata_fd.write("{}\n".format(json.dumps(info)))


def verify_remote(src):
    if '_CONDOR_JOB_AD' not in os.environ:
        print("This executable must be run within the HTCondor runtime environment.")
        sys.exit(1)

    logging.info("About to verify %s", src)
    src_fd = open(src, "r")
    file_size = os.fstat(src_fd.fileno()).st_size
    logging.info("There are %.2fMB to verify", file_size / 1024. / 1024.)
    last_log = time.time()
    buf = src_fd.read(1024*1024)
    hash_obj = hashlib.sha1()
    byte_count = len(buf)
    while len(buf) > 0:
        hash_obj.update(buf)
        buf = src_fd.read(1024*1024)
        now = time.time()
        if (now - last_log > 5):
            logging.info("Copied %.2f of %.2fMB; %.1f%% done", byte_count / 1024. / 1024., file_size / 1024. / 1024.,
                (byte_count / float(file_size)) * 100)
            last_log = now
        byte_count += len(buf)

    src_fd.close()
    logging.info("Checksum computation complete")

    logging.info("File metadata: hash=%s, size=%d", hash_obj.hexdigest(), byte_count)
    with open("metadata", "w") as metadata_fd:
        if simple_fname(src):
            metadata_fd.write("{} {} {}\n".format(src, hash_obj.hexdigest(), byte_count).encode('utf-8'))
        else:
            info = {"name": src, "digest": hash_obj.hexdigest(), "size": byte_count}
            metadata_fd.write("{}\n".format(json.dumps(info)))


def verify(dest_prefix, dest, metadata, metadata_summary):
    with open(metadata, "r") as metadata_fd:
        if os.fstat(metadata_fd.fileno()).st_size > 16384:
            logging.error("Metadata file is too large")
            sys.exit(1)
        contents = metadata_fd.read()
        contents = contents.strip()
        if not contents:
            logging.error("Metadata file is empty")
            sys.exit(1)
        if contents[0] == '{':
            info = json.loads(contents)
            if 'name' not in info or 'digest' not in info or 'size' not in info:
                logging.error("Metadata file format incorrect; missing keys")
                sys.exit(1)
            src_fname = info['name']
            src_hexdigest = info['digest'].encode("ascii")
            src_size = int(info['size'])
        else:
            info = contents.strip().split()
            if len(info) != 3:
                logging.error("Metadata file format incorrect")
                sys.exit(1)
            src_fname = info[0].decode('utf-8')
            src_hexdigest = info[1]
            src_size = int(info[2])

    relative_fname = dest[len(dest_prefix) + 1:]

    logging.info("About to verify contents of %s", dest)
    dest_fd = open(dest, "r")
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
            " source file %s (digest %s)", dest, src_hexdigest, src_fname, hash_obj.hexdigest())
        sys.exit(1)
    else:
        logging.info("File verification successful: Destination (%s) and source (%s) have matching"
            " SHA1 digest (%s)", dest, src_fname, src_hexdigest)

    with open(metadata_summary, "a") as md_fd:
        if simple_fname(relative_fname):
            md_fd.write("TRANSFER_VERIFIED {} {} {} {}\n".format(relative_fname, src_hexdigest, src_size, int(time.time())).encode('utf-8'))
        else:
            info = {"name": relative_fname, "digest": src_hexdigest, "size": src_size, "timestamp": int(time.time())}
            md_fd.write("TRANSFER_VERIFIED {}\n".format(json.dumps(info)))
        os.fsync(md_fd.fileno())

    os.unlink(metadata)
    if metadata.endswith(".metadata"):
        out_file = metadata[:-8] + "out"
        if os.path.exists(out_file):
            os.unlink(out_file)
        err_file = metadata[:-8] + "err"
        if os.path.exists(err_file):
            os.unlink(err_file)


def analyze(transfer_manifest):
    sync_request_start = None
    idx = -1
    sync_request = {'files': {}, 'xfer_files': set(), 'verified_files': {}}
    dest_dir = os.path.abspath(os.path.split(transfer_manifest)[0])
    sync_count = 0

    with open(transfer_manifest, "r") as fp:
        for line in fp.xreadlines():
            idx += 1
            info = line.strip().split()
            # Format: SYNC_REQUEST {} files_at_source={} files_to_transfer={} bytes_to_transfer={} files_to_verify={} bytes_to_verify={} timestamp={}
            if info[0] == 'SYNC_REQUEST':
                sync_count += 1
                #if sync_request_start is not None:
                #    logging.error("Sync request started at line %d but never finished; inconsistent log",
                #        sync_request_start)
                #    sys.exit(4)
                sync_request_start = idx
                for entry in info[2:]:
                    key, val = entry.split("=")
                    if key == 'timestamp':
                        continue
                    sync_request[key] = int(val)
            elif info[0] == 'TRANSFER_REQUEST' or info[0] == 'VERIFY_REQUEST': # Format: TRANSFER_REQUEST fname size
                if sync_request_start is None:
                    logging.error("Transfer request found at line %d before sync started; inconsistent log", idx)
                    sys.exit(4)
                if info[1][0] == '{':
                    local_info = json.loads(" ".join(info[1:]))
                    size = int(local_info['size'])
                    fname = local_info['name']
                else:
                    size = int(info[2])
                    fname = info[1]
                # File was previously verified.
                if fname in sync_request['verified_files'] and sync_request['verified_files'][fname] == size:
                    continue
                sync_request['files'][fname] = size
                if info[0] == 'TRANSFER_REQUEST':
                    if info[1][0] == '{':
                        local_info = json.loads(info[1])
                        sync_request['xfer_files'].add(local_info['name'])
                    else:
                        sync_request['xfer_files'].add(info[1])
            elif info[0] == 'TRANSFER_VERIFIED': # Format: TRANSFER_VERIFIED relative_fname hexdigest size timestamp:
                if sync_request_start is None:
                    logging.error("Transfer verification found at line %d before sync started; inconsistent log", idx)
                    sys.exit(4)
                if info[1][0] == '{':
                    local_info = json.loads(" ".join(info[1:]))
                    fname = local_info['name']
                    size = int(local_info['size'])
                else:
                    fname = info[1]
                    size = int(info[3])
                if fname in sync_request['verified_files'] and sync_request['verified_files'][fname] == size:
                    continue
                if fname not in sync_request['files']:
                    logging.error("File %s verified but was not requested.", fname)
                    sys.exit(4)
                if sync_request['files'][fname] != size:
                    logging.error("Verified file size %d of %s is different than anticipated", size, fname, sync_request['files'][fname])
                    sys.exit(4)
                try:
                    local_size = os.stat(os.path.join(dest_dir, fname)).st_size
                except OSError as oe:
                    logging.error("Unable to verify size of %s: %s", fname, str(oe))
                    sys.exit(4)
                if local_size != size:
                    logging.error("Local size of %d of %s does not match anticipated size %d.", local_size, fname, size)
                    sys.exit(4)
                if fname in sync_request['xfer_files']:
                    sync_request['files_to_transfer'] -= 1
                    sync_request['bytes_to_transfer'] -= size
                else:
                    sync_request['files_to_verify'] -= 1
                    sync_request['bytes_to_verify'] -= size
                del sync_request['files'][fname]
                sync_request['verified_files'][fname] = size
            elif info[0] == 'SYNC_DONE':
                if sync_request_start is None:
                    logging.error("Transfer request found at line %d before sync started; inconsistent log", idx)
                    sys.exit(4)

                if sync_request['files_to_verify'] or sync_request['bytes_to_verify'] or sync_request['files'] or \
                        sync_request['files_to_transfer'] or sync_request['bytes_to_transfer']:
                    logging.error("SYNC_DONE but there is work remaining: %s", str(sync_request))
                    sys.exit(4)
                sync_request_start = None
                sync_request = {'files': {}, 'xfer_files': set(), 'verified_files': {}}
        if sync_request_start is not None and (sync_request['files_to_verify'] or sync_request['bytes_to_verify'] or sync_request['files'] or \
                sync_request['files_to_transfer'] or sync_request['bytes_to_transfer']):
            logging.error("Sync not done! Work remaining.")
            logging.error("- Files to transfer: %s (bytes %d)", sync_request['files_to_transfer'], sync_request['bytes_to_transfer'])
            logging.error("- Files to verify: %s (bytes %d)", sync_request['files_to_verify'], sync_request['bytes_to_verify'])
            logging.error("Inconsistent files: {}".format(str(sync_request['files'])))
            sys.exit(4)
    if sync_request_start is not None:
        with open(transfer_manifest, "a") as fp:
            fp.write("SYNC_DONE {}\n".format(int(time.time())))
        print("Synchronization done; verification complete.")
    elif sync_count:
        print("All synchronizations done; verification complete")
    else:
        logging.error("No synchronization found in manifest.")
        sys.exit(1)


def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    args = parse_args()

    if args.cmd == "sync":
        if args.unique_id:
            schedd = htcondor.Schedd()
            if len(schedd.query(constraint = 'UniqueId == "{}" && JobStatus =!= 4'.format(args.unique_id),
                                attr_list = [], limit = 1)) > 0:
                logging.warning('Jobs already found in queue with UniqueId == "%s", exiting', args.unique_id)
                sys.exit()
        working_dir = args.working_dir if args.working_dir else os.getcwd()
        print("Will synchronize %s at source to %s at destination" % (args.src, args.dest))
        cluster_id = submit_parent_dag(working_dir, args.src, os.path.abspath(args.dest),
            requirements=read_requirements_file(args.requirements_file) or args.requirements, test_mode=args.test_mode,
            unique_id=args.unique_id)
        print("Parent job running in cluster %d" % cluster_id)
    elif args.cmd == "generate":
        logging.info("Generating file listing for %s", args.src)
        generate_file_listing(args.src, "source_manifest.txt", test_mode=args.test_mode)
    elif args.cmd == "write_subdag":
        logging.info("Generating SUBGDAG for transfer of %s->%s", args.source_prefix, args.dest_prefix)
        write_subdag(args.source_prefix, args.source_manifest, args.dest_prefix, args.dest_manifest, args.transfer_manifest,
            requirements=read_requirements_file(args.requirements_file) or args.requirements, test_mode=args.test_mode,
            unique_id=args.unique_id)
    elif args.cmd == "exec":
        xfer_exec(args.src)
    elif args.cmd == "verify":
        with open(args.json, "r") as fp:
            cmd_info = json.load(fp)
        info = cmd_info[args.fileid]
        verify(info['dest_prefix'], info['dest'], '{}.metadata'.format(info['src_file_noslash']), info['transfer_manifest'])
    elif args.cmd == "verify_remote":
        verify_remote(args.src)
    elif args.cmd == "analyze":
        analyze(args.transfer_manifest)

if __name__ == '__main__':
    main()
