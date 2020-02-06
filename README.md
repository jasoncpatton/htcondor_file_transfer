
HTCondor Transfer Data
======================

This repository contains some simple files and scripts to help move and
synchronize modest amounts of data (several TBs) between two HTCondor hosts.

Examples
========

`./xfer.py sync /home/imaging_user/my_new_images /mnt/my_cold_storage --requirements='UniqueName == "TestMachine01"'`