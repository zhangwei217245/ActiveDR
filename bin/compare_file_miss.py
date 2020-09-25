#!/usr/bin/env python
import os, fnmatch, sys
import argparse
import datetime

parent_dir = os.path.abspath(os.path.pardir)
sys.path.insert(0, parent_dir)

from lib.data_source.ornl.UserActivityAnalyzer import *
from lib.data_source.ornl.LightweightPurgeSimulator import *

def process_console_args():
    parser = argparse.ArgumentParser('compare_file_miss.py')
    parser.add_argument('-p', '--policy', metavar='<a/f>', default="f",
                        help='''policy, a=ActiveDR, f=Fixed-lifetime ''')
    parser.add_argument('-l', '--lifetime', metavar='lifetime', default="7",
                        help='''number of days in a single period''')
    args = parser.parse_args()
    return args

def main():
    args = process_console_args()

    activity_trace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/data/ornl/constellation"
    actTraceLoader = ActivityTraceLoader(trace_base_dir=activity_trace_dir)

    policy = None
    if args.policy=='a':
        policy = ActiveDR(activity_trace_loader=actTraceLoader, lifetime=int(args.lifetime))
    if args.policy=='f':
        policy = Fixed_Purge_Policy(activity_trace_loader=actTraceLoader, lifetime=int(args.lifetime))

    if policy is not None:
        simulator = LightweightPurgeSimulator(purge_policy=policy)
        simulator.simulate_2_year()

        for i in range(len(simulator.important_miss_ratios)-1):
            lower_bound = simulator.important_miss_ratios[i] 
            upper_bound = simulator.important_miss_ratios[i+1]
            print("file miss ratio between {}% and {}% occurred in {} days".format(lower_bound, upper_bound, simulator.miss_count_array[i]))

if __name__ == "__main__":
    main()