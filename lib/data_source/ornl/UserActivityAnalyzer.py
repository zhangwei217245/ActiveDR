# Author:  Wei Zhang

import hashlib, sys, os.path
import networkx as nx
import pandas as pd
import hashlib
import calendar
import datetime
import gc
import math
from sortedcontainers import SortedKeyList
# from codetiming import Timer
# from memory_profiler import profile

# from datetime import datetime
from lib.data_source.csv.CSVReader import CSVReader

class Publication(object):
    def __init__(self, publication_idx,num_authors,citation,month,year,impact_factor):
        self.publication_idx = publication_idx
        self.num_authors = num_authors if num_authors!=0 else 1
        self.citation = citation
        self.month = month
        self.year = year
        self.impact_factor = impact_factor

    def get_citation(self):
        return self.citation

    def get_impact_factor(self):
        if isinstance(self.impact_factor, float)==False:
            return 1.0
        return float(str(self.impact_factor))

    def get_month_ending_timestamp(self):
        last_date = calendar.monthrange(self.year, self.month)[1]
        d = datetime.datetime(self.year, self.month, last_date, 23, 59, 59)
        return d.timestamp()

class UserPubEdge(object):
    def __init__(self, rank, month, year, pub):
        self.rank = rank
        self.month = month
        self.year = year
        self.pub = pub
    
    def get_contribution_rate(self):
        return (self.pub.num_authors + 1 - self.rank)/(sum(range(self.pub.num_authors+1))/self.pub.num_authors)
    
    def get_month_ending_timestamp(self):
        last_date = calendar.monthrange(self.year, self.month)[1]
        d = datetime.datetime(self.year, self.month, last_date, 23, 59, 59)
        return d.timestamp()

    def get_pub_contribution(self):
        return self.get_contribution_rate() * self.pub.get_impact_factor() * 1000

    def get_activity_timestamp(self):
        return self.get_month_ending_timestamp()

    def get_activeness(self):
        return self.get_pub_contribution()

    def is_in_date_range(self, ts_range):
        return ts_range[0] <= self.get_activity_timestamp() and self.get_activity_timestamp() < ts_range[1]


class Job(object):
    def __init__(self,username,jobID,starttime,endtime,job_runtime_requested,num_Nodes,exit_Code):
        self.username = username
        self.job_id = jobID
        self.start_time = starttime
        self.end_time = endtime
        self.job_runtime_requested = job_runtime_requested
        self.num_nodes = num_Nodes
        self.exit_code = exit_Code

    def get_job_run_time(self):
        return self.end_time-self.start_time

    def get_job_node_time(self):
        return self.get_job_run_time() * self.num_nodes

    def get_job_time_exaggeration_rate(self):
        return self.job_runtime_requested/self.get_job_run_time()

    def get_activity_timestamp(self):
        return self.start_time
        
    def get_activeness(self):
        return self.get_job_node_time()

    def exit_normally(self):
        return True if self.exit_code == 0 else False

    def is_in_date_range(self, ts_range):        
        return ts_range[0] <= self.get_activity_timestamp() and self.get_activity_timestamp() < ts_range[1]

class User(object):
    def __init__(self, userID, username):
        self.userID = userID
        self.username = username
        self.node_id = None
        self.un_hash = hashlib.sha1(self.username.encode('utf-8')).hexdigest() if isinstance(username, str) else None
        self.pub_edges = SortedKeyList([], key=lambda p:p.get_month_ending_timestamp())
        self.jobs = SortedKeyList([], key=lambda j:j.start_time)
        self.pub_activeness = 0.0
        self.job_activeness = 0.0
        self.activeness = (self.job_activeness, self.pub_activeness)
        self.pub_active_v = 0.0
        self.job_active_v = 0.0
        self.active_v = (self.job_active_v, self.pub_active_v)
        self.num_purged_files = {'fixed':[0, 0, 0, 0], 'ares':[0, 0, 0, 0]}
        self.num_retained_files = {'fixed':[0, 0, 0, 0], 'ares':[0, 0, 0, 0]}
        self.size_purged_files = {'fixed':[0, 0, 0, 0], 'ares':[0, 0, 0, 0]}
        self.size_retained_files = {'fixed':[0, 0, 0, 0], 'ares':[0, 0, 0, 0]}
    
    def set_node_id(self, node_id):
        self.node_id = node_id

    def add_pub_edge(self, pub_edge):
        self.pub_edges.add(pub_edge)

    def add_job(self, job):
        self.jobs.add(job)

    def get_activeness_tuple(self):
        self.activeness = (self.job_activeness, self.pub_activeness)
        return self.activeness

    def get_active_v_tuple(self):
        self.active_v = (self.job_active_v, self.pub_active_v)
        return self.active_v

    def is_both_active(self):
        return self.job_activeness >= 1 and self.pub_activeness >= 1

    def is_job_active_only(self):
        return self.job_activeness >=1 and self.pub_activeness < 1
    
    def is_pub_active_only(self):
        return self.job_activeness < 1 and self.pub_activeness >=1
    
    def is_both_inactive(self):
        return self.job_activeness < 1 and self.pub_activeness < 1

    def get_uid_tuple(self):
        return (self.userID, self.username, self.un_hash, self.node_id)

    def is_job_active_match(self):
        return self.job_activeness >=1 and self.job_active_v >=1

    def is_pub_active_match(self):
        return self.pub_activeness >=1 and self.pub_active_v >=1

    def get_lifetime_coefficient(self):
        if self.is_both_active():
            return self.job_activeness * self.pub_activeness
        elif self.is_job_active_only():
            return self.job_activeness
        elif self.is_pub_active_only():
            return self.pub_activeness
        elif self.is_both_inactive():
            return 1
        else:
            return 1
class ActivenessTimeProperties(object):
    def __init__(self, timestamp, days_in_period):
        self.num_job_periods = 10
        self.num_pub_periods = 10
        self.job_period_len = days_in_period * 24 * 3600
        self.pub_period_len = 180 * 24 * 3600
        self.job_ts_range=(timestamp - self.job_period_len*self.num_job_periods, timestamp)
        self.pub_ts_range=(timestamp - self.pub_period_len*self.num_pub_periods, timestamp)
        self.job_ts_range_v = (timestamp, timestamp + self.job_period_len)
        self.pub_ts_range_v = (timestamp, timestamp + self.pub_period_len)


class ActivityTraceLoader(object):
    
    def __init__(self):
        self.user_file_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/users-20160530.csv"
        self.missing_uids_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/missing_uids.csv"
        self.user_node_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/node_user.csv"
        self.job_file_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/2013_2016_job.csv"
        self.job_node_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/node_job.csv"
        self.pub_node_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/node_publication.csv"
        self.edge_written_by_path = "/global/cscratch1/sd/wzhang5/data/recsys/constellation/edge_writtenBy.csv"
        self.userNameMap={}
        self.userNodeMap={}
        self.pubNodeMap={}

    def _on_missing_user_row(self, row):
        self.userNameMap[row.username]=User(row.uid, row.username)

    def _on_user_row(self, row):
        self.userNameMap[row.username]=User(row.userID, row.username)

    def _on_user_node_row(self, row):
        if isinstance(row.username, float):
            u=User(-1, "")
            u.set_node_id(row.user_idx)
            self.userNodeMap[row.user_idx]=u
        else:
            user = self.userNameMap.get(row.username)
            if user is None:
                print("user not found", row )
                u=User(-1, row.username)
                u.set_node_id(row.user_idx)
                self.userNodeMap[row.user_idx]=u
                self.userNameMap[row.username]=u
            else:
                user.set_node_id(row.user_idx)
                self.userNodeMap[row.user_idx]=user

    def _on_pub_node_row(self, row):
        self.pubNodeMap[row.publication_idx]=Publication(row.publication_idx,
            len(row.authors.split(';')), row.citation, row.month, row.year, row.impact_factor)

    def _on_edge_user_pub_row(self, row):
        user_idx = row.to_node_id
        pub_idx = row.from_node_id
        user = self.userNodeMap.get(user_idx)
        pub = self.pubNodeMap.get(pub_idx)
        user.add_pub_edge(UserPubEdge(row.rank, row.month, row.year, pub))

    def _on_job_row(self, row):
        job = Job(row.username, row.jobID, row.starttime, row.endtime,row.job_runtime_requested,
            row.num_Nodes, row.exit_Code)
        user = self.userNameMap.get(row.username)
        if user is None:
            print("User ", row.username, "not found,  jobid = ",row.jobID)
            self.userNameMap[row.username]=User(-1, row.username)
        else:
            user.add_job(job)

     # @Timer(name='load_users')
    def load_users(self):
        mu_csv_reader = CSVReader(self.missing_uids_path, self._on_missing_user_row, sep=':', 
            header=0, compression=None, 
            usecols=["uid","username"])
        mu_df = mu_csv_reader.load_csv()
        mu_csv_reader.iter_csv_rows()

        uf_csv_reader = CSVReader(self.user_file_path, self._on_user_row, sep=';', 
            header=0, compression=None, 
            usecols=["userID","username"])
        userfile_df = uf_csv_reader.load_csv()
        uf_csv_reader.iter_csv_rows()

        un_csv_reader = CSVReader(self.user_node_path, self._on_user_node_row, sep=',', 
            header=0, compression=None, 
            usecols=["user_idx","username"])
        usernode_df = un_csv_reader.load_csv()
        un_csv_reader.iter_csv_rows()

        del [[mu_df,userfile_df,usernode_df]]
        gc.collect()
        
    # @Timer(name='load_pubs')
    def load_publications(self):
        pub_csv_reader = CSVReader(self.pub_node_path, self._on_pub_node_row, sep=',', 
            header=0, compression=None, 
            usecols=["publication_idx","authors","citation","month","year","impact_factor"])
        pubnode_df = pub_csv_reader.load_csv()
        pub_csv_reader.iter_csv_rows()

        edge_writtenBy_csv_reader = CSVReader(self.edge_written_by_path, self._on_edge_user_pub_row, sep=',', 
            header=0, compression=None, 
            usecols=["from_node_id","to_node_id","rank","month","year"])
        edge_writtenBy_df = edge_writtenBy_csv_reader.load_csv()
        edge_writtenBy_csv_reader.iter_csv_rows()

        del [[pubnode_df, edge_writtenBy_df]]
        gc.collect()


    # @Timer(name='load_jobs')
    def load_jobs(self):
        job_csv_reader = CSVReader(self.job_file_path, self._on_job_row, sep=';', 
            header=0, compression=None, 
            usecols=["username","jobID","starttime","endtime","job_runtime_requested","num_Nodes","exit_Code"])
        job_df = job_csv_reader.load_csv()
        job_csv_reader.iter_csv_rows()

        del [[job_df]]
        gc.collect()

    def load_activity_data(self):
        print("loading user data...")
        self.load_users()
        print("loading publication data...")
        self.load_publications()
        print("loading job data...")
        self.load_jobs()
        return [self.userNameMap, self.userNodeMap, self.pubNodeMap]

class UserActivityAnalyzer(object):
    def __init__(self, rank, size, activity_data, timestamp, days_in_period):
        self.rank = rank
        self.size = size
        self.userNameMap=activity_data[0]
        self.userNodeMap=activity_data[1]
        self.pubNodeMap=activity_data[2]
        self.time_spec = ActivenessTimeProperties(timestamp, days_in_period)

    def get_ts_range(self, obj_type):
        if obj_type == 'job':
            return self.time_spec.job_ts_range
        if obj_type == 'pub':
            return self.time_spec.pub_ts_range
        if obj_type == 'job_v':
            return self.time_spec.job_ts_range_v
        if obj_type == 'pub_v':
            return self.time_spec.pub_ts_range_v

    def get_period_len(self, obj_type):
        if obj_type == 'job' or obj_type == 'job_v':
            return self.time_spec.job_period_len
        if obj_type == 'pub' or obj_type == 'pub_v':
            return self.time_spec.pub_period_len

    def get_user_activeness(self, obj_arr_par, obj_type):
        obj_arr = list(filter(lambda o:o.is_in_date_range(self.get_ts_range(obj_type)), obj_arr_par))
        if len(obj_arr) == 0:
            return 0.0
        else:
            user_total_activeness = sum(list(map(lambda o:o.get_activeness(), obj_arr)))
            user_num_periods = math.ceil((self.get_ts_range(obj_type)[1]-obj_arr[0].get_activity_timestamp())/self.get_period_len(obj_type))
            user_avg_activeness_per_period = user_total_activeness/user_num_periods if user_num_periods!=0 else user_total_activeness
            activeness = 1
            period = 1
            base = 0
            i = 0

            for o in obj_arr:
                exp = user_num_periods - int(math.ceil((self.get_ts_range(obj_type)[1]-o.get_activity_timestamp())/self.get_period_len(obj_type)))+1
                if i < len(obj_arr):
                    if exp == period: # accumulate activeness base in a single period
                        base += (o.get_activeness()/user_avg_activeness_per_period if user_avg_activeness_per_period!=0 else 0.0) 
                    elif exp > period: # when it comes to a new period, flush activeness result, and opens a new period
                        activeness *= pow(base, 1+period/2)
                        period=exp
                        base = (o.get_activeness()/user_avg_activeness_per_period if user_avg_activeness_per_period!=0 else 0.0)
                i+=1
                if i == len(obj_arr):
                    activeness *= pow(base, 1+period/2)
            return activeness


    # @Timer(name='ana_activeness')
    def analyze_activeness(self):
        result={}
        for u in self.userNameMap.values():
            # analyze pub first
            try:
                u.job_activeness = self.get_user_activeness(u.jobs, 'job')
                u.pub_activeness = self.get_user_activeness(u.pub_edges, 'pub')
                u.job_activeness = 0.0 if math.isnan(u.job_activeness) else u.job_activeness
                u.pub_activeness = 0.0 if math.isnan(u.pub_activeness) else u.pub_activeness

                u.job_active_v = self.get_user_activeness(u.jobs, 'job_v')
                u.pub_active_v = self.get_user_activeness(u.pub_edges, 'pub_v')
                u.job_active_v = 0.0 if math.isnan(u.job_active_v) else u.job_active_v
                u.pub_active_v = 0.0 if math.isnan(u.pub_active_v) else u.pub_active_v
            except TypeError:
                print("rank {} all activeness of {} {} fallback to 0.0 due to TypeError".format(self.rank, u.userID, u.username))
                u.job_activeness = 0.0
                u.pub_activeness = 0.0
                u.job_active_v = 0.0
                u.pub_active_v = 0.0

            result[u.userID] = u
        return result

    # @Timer(name='sort_users')
    def get_sorted_user_list_by_activeness(self, user_dict=None):
        userdict = self.userNameMap if user_dict is None else user_dict
        # divide entire user list as :
        # both active, job active only, pub active only, both inactive
        both_active = list(filter(lambda u:u.is_both_active(), userdict.values()))
        both_active.sort(reverse=True, key=lambda u:u.get_activeness_tuple())

        job_active_only = list(filter(lambda u:u.is_job_active_only(), userdict.values()))
        job_active_only.sort(reverse=True, key=lambda u:u.get_activeness_tuple())

        pub_active_only = list(filter(lambda u:u.is_pub_active_only(), userdict.values()))
        pub_active_only.sort(reverse=True, key=lambda u:u.get_activeness_tuple())

        both_inactive = list(filter(lambda u:u.is_both_inactive(), userdict.values()))
        both_inactive.sort(reverse=False, key=lambda u:u.get_activeness_tuple())

        return (both_active, job_active_only, pub_active_only, both_inactive)

    # @profile
    def run(self):
        print("start analysis...")
        return (self.analyze_activeness(), self.get_sorted_user_list_by_activeness())