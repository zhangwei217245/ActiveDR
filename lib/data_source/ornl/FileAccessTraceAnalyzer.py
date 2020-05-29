# Author:  Wei Zhang


import hashlib, sys, os.path
# from datetime import datetime
from lib.data_source.csv.CSVReader import CSVReader


class FileAccessTraceAnalyzer(object):

    def _sort_time_dict(self, el):
        return el[1]

    def _find_common_parent_dir(self, username, op_type, new_path):
        rst = self.curr_data_path
        if username==self.curr_user and op_type == self.op_type:
            while not (new_path.startswith(rst) and self.last_file_path.startswith(rst)):
                rst = os.path.dirname(rst)
        return rst

    # this function tries to sum up the values of the same key in two directory and merge the result into dst.  
    def _sum_dict(self, src, dst):
        if dst is None or src is None:
            return dst
        
        for k, v in src.items():
            if dst.get(k) is not None:
                dst[k] += v
            else:
                dsk[k] = v
        return dst
    

    def _on_row(self, row):

        # if Atime is the latest, we are sure that the file has been read
        # Ctime means metadata change, or file content change
        # Mtime means actual data change. 
        # When file/dir is created, all three time will be recorded as the current time.
        op_type = 'unknown'
        username = str(row.Uid) if self.user_dict.get(row.Uid) is None else self.user_dict[row.Uid]['username']

        time_list = [('Atime',row.Atime), ('Ctime',row.Ctime), ('Mtime',row.Mtime)]
        time_list.sort(reverse=True, key=self._sort_time_dict)
        # determine file creation:
        if time_list[0][1]==time_list[1][1] and time_list[1][1]==time_list[2][1]:
            op_type = "create"
        else:
            if time_list[0][0] == 'Mtime':
                op_type = "write"
            if time_list[0][0] == 'Atime':
                op_type = "read"
            if time_list[0][0] == 'Ctime':
                op_type = "meta"

        # a new file is encountered. 
        if self.last_file_path is None:
            self.last_file_path = row.PATH
            self.curr_data_path = os.path.dirname(row.PATH)
            self.curr_user = username
            self.curr_uid = row.Uid
            self.op_type = op_type
            self.min_ts = time_list[0][1]
            self.max_ts = time_list[0][1]
            self.file_count = 1
        else:
            # user access the file under the same directory with the same operation
            if row.Uid == self.curr_uid and row.PATH.startswith(self.curr_data_path) and self.op_type == op_type:
                #only update timestamp
                self.file_count = self.file_count+1
                if self.min_ts > time_list[0][1]:
                    self.min_ts = time_list[0][1]
                if self.max_ts < time_list[0][1]:
                    self.max_ts = time_list[0][1]
            else: #user-changes or directory changes or operation changes
                data_path_md5 = hashlib.md5(self.curr_data_path.encode("utf-8")).hexdigest()
                # summarizing the recent few lines with operations on the files of the same directory
                self.outputFile.write("{},{},{},{},{},{},{}\n".format(self.curr_uid, self.curr_user, \
                    self.min_ts, self.max_ts, self.op_type, data_path_md5, self.file_count))
                
                # if the edge has been created, add time stamp? No. We should add another edge.
                if self.graph.has_edge(self.curr_uid, data_path_md5, self.op_type):
                    edge_data = self.graph.get_edge_data(self.curr_uid, data_path_md5, self.op_type)
                    edge_data['ts'].append((self.min_ts,self.max_ts - self.min_ts))
                    # edge_data['min_ts'] = self.min_ts if self.min_ts < edge_data['min_ts'] else edge_data['min_ts']
                    # edge_data['max_ts'] = self.max_ts if self.max_ts > edge_data['max_ts'] else edge_data['max_ts']
                    edge_data['count'] += 1
                else:
                    self.graph.add_edge(self.curr_uid, data_path_md5, self.op_type, op_type=self.op_type, count=1, fc=self.file_count,
                    ts=[(self.min_ts, self.max_ts-self.min_ts)])
                # process OST graph
                if isinstance(row.OST, str) and row.OST is not None:
                    for x in row.OST.split('|'):
                        ost_id = 'o'+x.split(':')[0]
                        # update OST dir file count
                        self.graph.add_edge(ost_id, data_path_md5, 'OST-DIR', fc=self.file_count)
                        # update user-OST access graph
                        if self.graph.has_edge(self.curr_uid, ost_id, 'UID-OST'):
                            edge_data = self.graph.get_edge_data(self.curr_uid, ost_id, 'UID-OST')
                            edge_data['ts'].append((self.min_ts,self.max_ts - self.min_ts))
                            edge_data['count'] += 1
                        else:
                            self.graph.add_edge(self.curr_uid, ost_id, 'UID-OST', ts=[(self.min_ts, self.max_ts-self.min_ts)], count=1)

                self.file_count = 1
                self.curr_user = username
                self.curr_uid = row.Uid
                self.op_type=op_type
                common_data_path = self._find_common_parent_dir(username, op_type, row.PATH)
                self.curr_data_path = common_data_path if common_data_path!="/" else os.path.dirname(row.PATH)
                self.min_ts = time_list[0][1]
                self.max_ts = time_list[0][1]
            self.last_file_path = row.PATH

    def __init__(self, source_path, user_csv_path, output_path, graph, **kwargs):
        self.source = source_path
        self.output = output_path
        self.graph = graph
        self.csv_reader = CSVReader(self.source, self._on_row, **kwargs)
        user_csv_loader = CSVReader(user_csv_path, None, sep=';', index_col=0, usecols=['userID', 'username'])
        self.user_dict = user_csv_loader.load_csv().T.to_dict()
        self.curr_user = None
        self.curr_uid = None
        self.unknown_user_count = 0
        self.op_type = None
        self.min_ts = 0
        self.max_ts = 0
        self.file_count = 0
        self.last_file_path = None
        self.curr_data_path = None
        self.outputFile = sys.stdout if self.output is None else open(self.output, 'a')
        # print(self.user_dict)

    def read_source_csv(self):
        # print(self.source)
        dataframe1 = self.csv_reader.load_csv()
        self.csv_reader.iter_csv_rows()
        if self.outputFile != sys.stdout:
            self.outputFile.close()
        # print("create:{}%, write:{}%, read:{}%, meta:{}%, all_four:{}".format(self.create_count*100/self.total_count, 
        # self.write_count*100/self.total_count, self.read_count*100/self.total_count, 
        # self.meta_count*100/self.total_count, 
        # (self.write_count+self.read_count+self.create_count+self.meta_count) == self.total_count))
