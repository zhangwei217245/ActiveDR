import unittest
import sys, os
import networkx as nx

from lib.data_source.ornl.FileAccessTraceAnalyzer import *


# @unittest.skip("skipping this unittest for now")
class FileAccessTraceAnalyzerTestCase(unittest.TestCase):

    def setUp(self):
        self.graph = nx.MultiDiGraph()
        self._parent_dir = os.path.abspath(os.path.curdir)
        sys.path.insert(0, self._parent_dir)
        source_path = self._parent_dir + "/data/ornl/raw/20150112/part-00000-26783723-4b6a-4838-839a-c05517816f83-c000.csv.gz"
        user_csv_path = self._parent_dir+"/data/ornl/raw/users-20160530.csv"
        output_path = self._parent_dir + "/data/ornl/output/final.csv"
        self.analyzer = FileAccessTraceAnalyzer(source_path, user_csv_path, None, self.graph,  sep=',', 
        header=None, compression='gzip', names=["Atime","Ctime","Mtime","OST","area","gid","itemsize",
        "PATH","permission","project","Uid","unknown","snapshot_date"], usecols=["Atime","Ctime","Mtime","PATH","Uid","snapshot_date"])
        return super().setUp()

    def test_read_source_csv(self):
        self.analyzer.read_source_csv()
        for u, v, k, d in self.graph.edges(data=True, keys=True):
            sys.stdout.write("{}|{}|{}|{}|{}|{}|{}\n".format(u, v, k, d['min_ts'], d['max_ts'], d['count'], d['fc']))
        # print(str(self.graph.edges(data=True, keys=True)))

    def tearDown(self):
        return super().tearDown()

if __name__ == '__main__':
    unittest.main()