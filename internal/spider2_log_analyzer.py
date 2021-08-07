#!/usr/bin/env python
import os, fnmatch, sys
import networkx as nx
import argparse
# import gzip

from mpi4py import MPI

parent_dir = os.path.abspath(os.path.pardir)
sys.path.insert(0, parent_dir)

from lib.data_source.ornl.FileAccessTraceAnalyzer import *


class Spider2Analyzer(object):

    def __init__(self, mpi_rank, mpi_size, input_dir, output_dir, year):
        self.mpi_rank = mpi_rank
        self.mpi_size = mpi_size
        self._parent_dir = os.path.abspath(os.path.pardir)
        sys.path.insert(0, self._parent_dir)
        self.spider_dir = input_dir
        self.output_dir = output_dir
        self.year = str(year)+"*"
        self.dir_name = "0000"
        self.graph = nx.MultiDiGraph()
        self.user_csv_path = self._parent_dir+"/data/ornl/raw/users-20160530.csv"

    def _clean_output(self, output_file_path):
        of = open(output_file_path, 'w')
        of.write("")
        of.close()

    def process_files_in_dir(self, date_dir_name):
        dir_path = self.spider_dir+'/'+date_dir_name
        self._clean_output(self.output_dir+'/'+date_dir_name+'.csv')
        listOfGzipCSVs = sorted([f for f in os.listdir(dir_path) if fnmatch.fnmatch(f, "part-*.csv.gz")])
        total_count = len(listOfGzipCSVs)
        count = 0
        for f in listOfGzipCSVs:
            analyzer = FileAccessTraceAnalyzer(dir_path+'/'+f, self.user_csv_path, self.output_dir+'/'+date_dir_name+'.csv', 
            self.graph, sep=',', 
            header=None, compression='gzip', names=["Atime","Ctime","Mtime","OST","area","gid","itemsize",
            "PATH","permission","project","Uid","unknown","snapshot_date"], 
            usecols=["Atime","Ctime","Mtime", "OST", "PATH","Uid"])
            analyzer.read_source_csv()
            count += 1
            print("Rank {} Progress: {}/{}".format(self.mpi_rank, count, total_count))

    def walk_spider_dirs(self):
        listOfFolders = sorted([f for f in os.listdir(self.spider_dir) if fnmatch.fnmatch(f, self.year)])
        count = 0
        for d in listOfFolders:
            if count % self.mpi_size == self.mpi_rank:
                print("rank {} processing {}".format(self.mpi_rank, d))
                self.dir_name=d
                self.process_files_in_dir(d)
            count+=1
        # output user dir file
        with open(self.output_dir+"/user_file_edge_rank"+str(self.dir_name)+".csv", 'wt') as ff:
            for u, v, k, d in self.graph.edges(data=True, keys=True):
                if k=='write' or k=='read' or k=='create' or k=='meta':
                    ff.write("{}|{}|{}|{}|{}|{}\n".format(u, v, k, d['ts'], d['count'],d['fc']))

        # output user-OST file
        with open(self.output_dir+"/user_OST_edge_rank"+str(self.dir_name)+".csv", 'wt') as ff:
            for u, v, k, d in self.graph.edges(data=True, keys=True):
                if k=='UID-OST':
                    ff.write("{}|{}|{}|{}\n".format(u, v, d['ts'], d['count']))

        # output OST-file file
        with open(self.output_dir+"/OST_DIR_edge_rank"+str(self.dir_name)+".csv", 'wt') as ff:
            for u, v, k, d in self.graph.edges(data=True, keys=True):
                if k=='OST-DIR':
                    ff.write("{}|{}|{}\n".format(u, v, d['fc']))


        
def process_console_args():
    parser = argparse.ArgumentParser('spider2_log_analyzer.py')
    parser.add_argument('-i', '--input', metavar='<input dir>', default="/global/cscratch1/sd/wzhang5/data/recsys/spider2_trace",
                        help='''The path of input directory''')
    parser.add_argument('-o', '--output', metavar='<output dir>', default="/global/cscratch1/sd/wzhang5/data/recsys/file_access",
                        help='''The path of output directory''')
    parser.add_argument('-y', '--year', metavar='<year>', default="2016",help='''year prefix of the trace directories''')
    args = parser.parse_args()
    return args

def main():
    mpi_rank = MPI.COMM_WORLD.Get_rank()
    mpi_size = MPI.COMM_WORLD.Get_size()
    args = process_console_args()
    spider2Analyzer = Spider2Analyzer(mpi_rank, mpi_size, args.input, args.output, args.year)
    spider2Analyzer.walk_spider_dirs()


if __name__ == "__main__":
    main()


