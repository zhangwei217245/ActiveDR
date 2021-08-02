#!/usr/bin/env python
import os, fnmatch, sys
import argparse
import datetime

# import gzip

parent_dir = os.path.abspath(os.path.pardir)
sys.path.insert(0, parent_dir)

from lib.data_source.ornl.PurgeSimulator import *



def process_console_args():
    parser = argparse.ArgumentParser('user_activity_analyzer.py')
    parser.add_argument('-d', '--date', metavar='<yyyyMMdd>', default="20160823",
                        help='''date when snapshot was taken''')
    parser.add_argument('-f', '--function', metavar='<sim/reducer>', default="sim",
                        help='''functionality''')
    parser.add_argument('-m', '--mode', metavar='<local/hpc>', default="hpc", help='''
                        specify if this is running on a local machine or an HPC environment''')
    # parser.add_argument('-l', '--len', metavar='<period length>', default="7",
    #                     help='''number of days in a single period''')
    args = parser.parse_args()
    return args

def main():
    mpi_rank = 0
    mpi_size = 1
    args = process_console_args()
    
    d = datetime.datetime.strptime(args.date+"235959" , "%Y%m%d%H%M%S")
    mode = args.mode
    data_base_dir = "/global/cscratch1/sd/wzhang5/data/recsys/" if (mode == 'hpc') else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/data"
    psim = PurgePolicySimulator(mpi_rank, mpi_size, args.date, d.timestamp(), False, data_base_dir, args.function)
    psim.run()

if __name__ == "__main__":
    main()