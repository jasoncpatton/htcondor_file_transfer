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
import sys
import time

import htcondor


PARENT_DAG = \
"""
# Gather the remote file listing.
JOB CalculateWork calc_work.sub DIR calc_work

# Gather the local file listing and write out the sub-DAG
# to perform the actual transfers
SCRIPT POST CalculateWork {exec_py} write_subdag {source_prefix} source_manifest.txt {dest_prefix} destination_manifest.txt {transfer_manifest} {other_args}

SUBDAG EXTERNAL DoXfers calc_work/do_work.dag

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

queue
"""

XFER_FILE_JOB = \
"""
universe = vanilla
executable = {}
output = $(src_file_noslash).out
error = $(src_file_noslash).err
log = xfer_file.log
arguments = exec $(src_file)
should_transfer_files = YES
transfer_output_files = file0, metadata
transfer_output_remaps = "file0 = $(dst_file); metadata = $(src_file_noslash).metadata"

queue
"""

XFER_VERIFY_JOB = \
"""
universe = vanilla
executable = {}
output = $(src_file_noslash).out
error = $(src_file_noslash).err
log = xfer_file.log
arguments = verify_remote $(src_file)
should_transfer_files = YES
transfer_output_files = metadata
transfer_output_remaps = "metadata = $(src_file_noslash).metadata"

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
SCRIPT POST xfer_{name} {xfer_py} verify {dest} {src_file_noslash}.metadata {transfer_manifest}

"""

DO_WORK_DAG_VERIFY_SNIPPET = \
"""
JOB verify_{name} verify_file.sub DIR calc_work
VARS verify_{name} src_file_noslash="{src_file_noslash}"
VARS verify_{name} src_file="{src_file}"
VARS verify_{name} dst_file="{dest}"
SCRIPT POST verify_{name} {xfer_py} verify {dest} {src_file_noslash}.metadata {transfer_manifest}

"""

def search_path(exec_name):
    for path in os.environ.get("PATH", "/usr/bin:/bin").split(":"):
        fname = os.path.join(path, exec_name)
        if os.access(fname, os.X_OK):
            return fname


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    parser_sync = subparsers.add_parser("sync")
    parser_sync.add_argument("src")
    parser_sync.add_argument("dest")
    parser_sync.add_argument("--working-dir", help="Directory to place working HTCondor files.",
        default="./scratch_dir", dest="working_dir")
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
    parser_subdag.add_argument("--test-mode", help="Testing mode (only transfers small files)",
        default=False, action="store_true", dest="test_mode")

    parser_exec = subparsers.add_parser("exec")
    parser_exec.add_argument("src")

    parser_verify_remote = subparsers.add_parser("verify_remote")
    parser_verify_remote.add_argument("src")

    parser_verify = subparsers.add_parser("verify")
    parser_verify.add_argument("dest")
    parser_verify.add_argument("metadata")
    parser_verify.add_argument("metadata_summary")
 
    return parser.parse_args()


def generate_file_listing(src, manifest, test_mode=False):
    with open(manifest, "w") as fp:
        for root, dirs, fnames in os.walk(src):
            for fname in fnames:
                full_fname = os.path.normpath(os.path.join(root, fname))
                size = os.stat(full_fname).st_size
                if test_mode and size > 50*1024*1024:
                    continue
                fp.write("{} {}\n".format(full_fname, size))


def submit_parent_dag(working_dir, source_dir, dest_dir, test_mode=False):
    try:
        os.makedirs(os.path.join(working_dir, "calc_work"))
    except OSError as oe:
        if oe.errno != errno.EEXIST:
            raise

    info = os.path.split(sys.argv[0])
    full_exec_path = os.path.join(os.path.abspath(info[0]), info[1])

    with open(os.path.join(working_dir, "xfer.dag"), "w") as fd:
        fd.write(PARENT_DAG.format(exec_py=full_exec_path, source_prefix=source_dir,
            dest_prefix=dest_dir, other_args="--test-mode" if test_mode else "",
            transfer_manifest=os.path.join(dest_dir, "transfer_manifest.txt")))

    with open(os.path.join(working_dir, "calc_work", "calc_work.sub"), "w") as fd:
        fd.write(CALC_WORK_JOB.format(exec_py=full_exec_path, source_dir=source_dir,
            other_args="--test-mode" if test_mode else ""))

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
            info = line.strip().split()
            if len(info) != 2:
                raise Exception("Manifest lines must have two columns")
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


def write_subdag(source_prefix, source_manifest, dest_prefix, dest_manifest, transfer_manifest, test_mode=False):
    src_files = parse_manifest(source_prefix, source_manifest, "Source")

    generate_file_listing(dest_prefix, "destination_manifest.txt")
    dest_files = parse_manifest(dest_prefix, "destination_manifest.txt", "Destination")

    files_to_xfer = set()
    for fname in src_files:
        if src_files[fname] != dest_files.get(dest_files, -1):
            files_to_xfer.add(fname)

    transfer_manifest = os.path.join(dest_prefix, "transfer_manifest.txt")
    if not os.path.exists(transfer_manifest):
        with open(transfer_manifest, "w") as fp:
            pass

    files_verified = set()
    with open(transfer_manifest, "r") as fp:
        for line in fp.readlines():
            info = line.strip().split()
            if info != 'TRANSFER_VERIFIED':
                continue
            if len(info) != 4:
                continue
            fname, hexdigest, size = info[1:]
            if !fname.startswith(source_prefix):
                logging.warning("Incorrect source prefix in transfer_manifest.txt; filename %s", fname)
                sys.exit(3)
            relative_fname = fname[len(source_prefix) + 1:]
            files_verified.add(realtive_fname)

    files_to_verify = set()
    for fname in src_files:
        if fname in files_to_xfer:
            continue
        if fname not in files_verified:
            files_to_verify.add(fname)

    info = os.path.split(sys.argv[0])
    full_exec_path = os.path.join(os.path.abspath(info[0]), info[1])

    with open("xfer_file.sub", "w") as fp:
        fp.write(XFER_FILE_JOB.format(full_exec_path))
    with open("verify_file.sub", "w") as fp:
        fp.write(VERIFY_FILE_JOB.format(full_exec_path))

    idx = 0
    dest_dirs = set()
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
            fp.write(DO_WORK_DAG_XFER_SNIPPET.format(name=idx, src_file=src_file,
                xfer_py=full_exec_path, src_file_noslash=src_file_noslash, dest=dest,
                transfer_manifest=transfer_manifest))

        idx = 0
        for fname in files_to_verify:
            idx += 1
            src_file = os.path.join(source_prefix, fname)
            src_file_noslash = fname.replace("/", "_")
            logging.info("File to verify: %s", src_file)
            fp.write(DO_WORK_DAG_VERIFY_SNIPPET.format(name=idx, src_file=src_file,
                xfer_py=full_exec_path, src_file_noslash=src_file_noslash, dest=dest,
                transfer_manifest=transfer_manifest))

    for dest_dir in dest_dirs:
        try:
            os.makedirs(dest_dir)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise


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
        metadata_fd.write("{} {} {}\n".format(src, hash_obj.hexdigest(), byte_count).encode('utf-8'))


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
        metadata_fd.write("{} {} {}\n".format(src, hash_obj.hexdigest(), byte_count).encode('utf-8'))


def verify(dest, metadata, metadata_summary):
    with open(metadata, "r") as metadata_fd:
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
        md_fd.write("TRANSFER_VERIFIED {} {} {}\n".format(src_fname, src_hexdigest, src_size).encode('utf-8'))
        os.fsync(md_fd.fileno())


def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    args = parse_args()

    if args.cmd == "sync":
        working_dir = args.working_dir if args.working_dir else os.getcwd()
        print("Will transfer %s at source to %s at destination" % (args.src, args.dest))
        cluster_id = submit_parent_dag(working_dir, args.src, os.path.abspath(args.dest), test_mode=args.test_mode)
        print("Parent job running in cluster %d" % cluster_id)
    elif args.cmd == "generate":
        logging.info("Generating file listing for %s", args.src)
        generate_file_listing(args.src, "source_manifest.txt", test_mode=args.test_mode)
    elif args.cmd == "write_subdag":
        logging.info("Generating SUBGDAG for transfer of %s->%s", args.source_prefix, args.dest_prefix)
        write_subdag(args.source_prefix, args.source_manifest, args.dest_prefix, args.dest_manifest, args.transfer_manifest, test_mode=args.test_mode)
    elif args.cmd == "exec":
        xfer_exec(args.src)
    elif args.cmd == "verify":
        verify(args.dest, args.metadata, args.metadata_summary)
    elif args.cmd == "verify_remote":
        verify_remote(args.src)

if __name__ == '__main__':
    main()
