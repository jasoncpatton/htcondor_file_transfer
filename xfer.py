#!/usr/bin/python

"""
Utilize HTCondor to transfer / synchronize a directory from a source on an
execute host to a local destination on the submit host.
"""

from __future__ import print_function, unicode_literals

import argparse
import errno
import logging
import os
import sys

import htcondor


PARENT_DAG = \
"""
# Gather the remote file listing.
JOB CalculateWork calc_work.sub DIR calc_work

# Gather the local file listing and write out the sub-DAG
# to perform the actual transfers
SCRIPT POST CalculateWork {exec_py} write_subdag {source_prefix} source_manifest.txt {dest_prefix} destination_manifest.txt

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
arguments = $(src_file)
should_transfer_files = YES
transfer_output_files = file0, metadata
transfer_output_remaps = "file0 = $(dst_file); metadata = $(src_file_noslash).metadata"

queue
"""

DO_WORK_DAG_HEADER = \
"""
CATEGORY ALL_NODES TRANSFER_JOBS
MAXJOBS TRANSFER_JOBS 1
"""

DO_WORK_DAG_SNIPPET = \
"""
JOB xfer_{name} xfer_file.sub DIR calc_work
VARS xfer_{name} src_file_noslash="{src_file_noslash}"
VARS xfer_{name} src_file="{src_file}"
VARS xfer_{name} dst_file="{dest}"
SCRIPT POST xfer_{name} {xfer_verify} {dest} {src_file_noslash}.metadata transfer_manifest.txt

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
        default=None, dest="working_dir")
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
            dest_prefix=dest_dir))

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


def write_subdag(source_prefix, source_manifest, dest_prefix, dest_manifest):
    src_files = parse_manifest(source_prefix, source_manifest, "Source")

    generate_file_listing(dest_prefix, "destination_manifest.txt")
    dest_files = parse_manifest(dest_prefix, "destination_manifest.txt", "Destination")

    src_files_set = set(src_files)
    dest_files_set = set(dest_files)
    files_to_xfer = src_files_set - dest_files_set

    # TODO: xfer_exec.py should be merged into this as well...
    dirpath = os.path.split(sys.argv[0])[0]
    full_exec_path = os.path.join(dirpath, "xfer_exec.py")

    with open("xfer_file.sub", "w") as fp:
        fp.write(XFER_FILE_JOB.format(full_exec_path))

    full_exec_path = os.path.join(dirpath, "xfer_verify.py")
    idx = 0

    dest_dirs = set()

    with open("do_work.dag", "w") as fp:
        fp.write(DO_WORK_DAG_HEADER)
        files_to_xfer = list(files_to_xfer)
        files_to_xfer.sort()
        for fname in files_to_xfer:
            idx += 1
            src_file = os.path.join(source_prefix, fname)
            src_file_noslash = fname.replace("/", "_")
            dest = os.path.join(dest_prefix, fname)
            dest_dirs.add(os.path.split(dest)[0])
            logging.info("File transfer to perform: %s->%s", src_file, dest)
            fp.write(DO_WORK_DAG_SNIPPET.format(name=idx, src_file=src_file,
                xfer_verify=full_exec_path, src_file_noslash=src_file_noslash, dest=dest))

    for dest_dir in dest_dirs:
        try:
            os.makedirs(dest_dir)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise

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
        write_subdag(args.source_prefix, args.source_manifest, args.dest_prefix, args.dest_manifest)

if __name__ == '__main__':
    main()
