import hashlib, sys, re, os, os.path, fnmatch, gc
import numpy as np
import pandas as pd
import time
from random import Random
from functools import reduce

# from datetime import datetime
from lib.data_source.csv.CSVReader import CSVReader
from lib.data_source.ornl.UserActivityAnalyzer import *

class RetentionPolicyResult(object):
    def __init__(self, policy_name="Dr. Ares", period_days=7):
        self.period_days = period_days
        self.policy_name = policy_name # or "Fixed-Lifetime"
        self.total_num_files_retained = 0
        self.total_num_files_removed = 0
        self.total_mb_retained = 0
        self.total_mb_removed = 0
        self.num_purged_files_for_4_groups = [0,0,0,0]
        self.num_retained_files_for_4_groups = [0,0,0,0]
        self.mb_purged_for_4_groups = [0,0,0,0]
        self.mb_retained_for_4_groups = [0,0,0,0]
        self.users_whose_files_removed = ({},{},{},{})
        self.users_whose_files_protected = ({},{},{},{})

class PurgePolicySimulator(object):
    
    def __init__(self, rank, size, date_str, date_timestamp, is_mpi, function='sim'):
        self.monitored_init(rank, size, date_str, date_timestamp, is_mpi, function)

    # @profile
    def monitored_init(self, rank, size, date_str, date_timestamp, is_mpi, function='sim'):
        self.rank = rank
        self.size = size
        self.function = function
        if is_mpi:
            from mpi4py import MPI
        self.file_size_rand = Random(x=5)
        self.spider_trace_root_path = "/global/cscratch1/sd/wzhang5/data/recsys/spider2_trace"
        self.output_root="/global/cscratch1/sd/wzhang5/data/recsys/purge_result" if (self.function=='reducer' or is_mpi) else "/global/cscratch1/sd/wzhang5/data/recsys/purge_result_2"
        self.date_str = date_str
        self.specified_date_timestamp = date_timestamp
        self.purge_target = 10000000 / self.size
        self.testing_periods=[7, 30, 60, 90] #if size > 1 else [7]
        self.policies = {}
        self.policies['fixed'] = list(map(lambda l:RetentionPolicyResult("Fixed-Lifetime",l), self.testing_periods))
        self.policies['ares'] = list(map(lambda l:RetentionPolicyResult("Dr. Ares",l), self.testing_periods))
        if self.function == 'sim':
            for i in range(0, self.size):
                if is_mpi:
                    MPI.COMM_WORLD.Barrier()
                if i == rank:
                    self.activity_data = ActivityTraceLoader().load_activity_data()
                    self.uaAnalyzers = list(map(lambda l:UserActivityAnalyzer(self.rank, self.size, self.activity_data, date_timestamp, l), self.testing_periods))
                    self.userIDMap = list(map(lambda a:a.run()[0], self.uaAnalyzers))

            self.user_ids_scanned_in_spider_trace=set()
            self.total_num_files = 0
            self.total_file_size = 0
        
    def __uagroup_id(self, user):
        if user.is_both_active():
            return 0
        elif user.is_job_active_only():
            return 1
        elif user.is_pub_active_only():
            return 2
        elif user.is_both_inactive():
            return 3
        else:
            return 3

    def simulate_purge(self, p, l, row, user, file_size):
        ua_g=self.__uagroup_id(user)
        user.num_purged_files[p][l]+=1
        user.size_purged_files[p][l]+=file_size
        self.policies[p][l].users_whose_files_removed[ua_g][row.Uid]=user
        self.policies[p][l].num_purged_files_for_4_groups[ua_g]+=1
        self.policies[p][l].total_num_files_removed+=1
        self.policies[p][l].mb_purged_for_4_groups[ua_g]+=file_size
        self.policies[p][l].total_mb_removed+=file_size

    def simulate_retain(self, p, l, row, user, file_size):
        ua_g=self.__uagroup_id(user)
        user.num_retained_files[p][l]+=1
        user.size_retained_files[p][l]+=file_size
        self.policies[p][l].users_whose_files_protected[ua_g][row.Uid]=user
        self.policies[p][l].num_retained_files_for_4_groups[ua_g]+=1
        self.policies[p][l].total_num_files_retained+=1
        self.policies[p][l].mb_retained_for_4_groups[ua_g]+=file_size
        self.policies[p][l].total_mb_retained+=file_size

    def run_fixed_lifetime_policy(self, time_list, row, file_size):
        p = 'fixed'
        for l in range(0, len(self.testing_periods)):
            # figure out user identity
            user = User(row.Uid, "") if self.userIDMap[l].get(row.Uid) is None else self.userIDMap[l].get(row.Uid)
            # if user is not None:
            if self.policies[p][l].total_mb_removed+file_size < self.purge_target and self.specified_date_timestamp - time_list[0][1] > self.uaAnalyzers[l].time_spec.job_period_len:
                self.simulate_purge(p, l, row, user, file_size)
            else:
                self.simulate_retain(p, l, row, user, file_size)

    def run_ares_policy(self, time_list, row, file_size):
        p='ares'
        for l in range(0, len(self.testing_periods)):
            user = User(row.Uid, "") if self.userIDMap[l].get(row.Uid) is None else self.userIDMap[l].get(row.Uid)
            if self.policies[p][l].total_mb_removed+file_size < self.purge_target and self.specified_date_timestamp - time_list[0][1] > self.uaAnalyzers[l].time_spec.job_period_len*user.get_lifetime_coefficient():
                self.simulate_purge(p,l,row,user,file_size)
            else:
                self.simulate_retain(p,l,row,user,file_size)

    def output_activeness(self):
        if self.rank == 0:
            for i in range(0, len(self.testing_periods)):
                with open(self.output_root+"/"+self.date_str+"_user_active_"+str(self.testing_periods[i])+"_days.csv", 'wt') as user_active_out:
                    user_active_out.write("uid,username,job,pub,job_v,pub_v,job_match,pub_match,scanned\n")
                    for u in self.userIDMap[i].values():
                        active_tuple = u.get_activeness_tuple()
                        active_v_tuple = u.get_active_v_tuple()
                        scanned = "Scanned" if u.userID in self.user_ids_scanned_in_spider_trace else "Unscanned"
                        user_active_out.write("{},{},{},{},{},{},{},{},{}\n".format(u.userID, u.username,
                        active_tuple[0], active_tuple[1],active_v_tuple[0],active_v_tuple[1],
                        u.is_job_active_match(),u.is_pub_active_match(),scanned))

    def output_rst(self):
        for p in ['fixed', 'ares']:
            with open(self.output_root+"/"+self.date_str+"_overall_"+p+"_rank_"+str(self.rank)+".csv", 'wt') as overall_out:
                overall_out.write("days,total_files,total_purge,total_retain,total_mb_purge,total_mb_retain,"+
                "fp_both_act,fp_job_act_only,fp_pub_act_only,fp_both_inact,"+
                "fr_both_act,fr_job_act_only,fr_pub_act_only,fr_both_inact,"+
                "sp_both_act,sp_job_act_only,sp_pub_act_only,sp_both_inact,"+
                "sr_both_act,sr_job_act_only,sr_pub_act_only,sr_both_inact,"+
                "up_both_act,up_job_act_only,up_pub_act_only,up_both_inact,"+
                "ur_both_act,ur_job_act_only,ur_pub_act_only,ur_both_inact\n")
                for i in range(0, len(self.testing_periods)):
                    rst = self.policies[p][i]
                    plen = self.testing_periods[i]
                    overall_out.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n"
                    .format(plen,self.total_num_files,rst.total_num_files_removed,rst.total_num_files_retained,rst.total_mb_removed,rst.total_mb_retained,
                    rst.num_purged_files_for_4_groups[0],rst.num_purged_files_for_4_groups[1],rst.num_purged_files_for_4_groups[2],rst.num_purged_files_for_4_groups[3],
                    rst.num_retained_files_for_4_groups[0],rst.num_retained_files_for_4_groups[1],rst.num_retained_files_for_4_groups[2],rst.num_retained_files_for_4_groups[3],
                    rst.mb_purged_for_4_groups[0],rst.mb_purged_for_4_groups[1],rst.mb_purged_for_4_groups[2],rst.mb_purged_for_4_groups[3],
                    rst.mb_retained_for_4_groups[0],rst.mb_retained_for_4_groups[1],rst.mb_retained_for_4_groups[2],rst.mb_retained_for_4_groups[3],
                    len(rst.users_whose_files_removed[0]),len(rst.users_whose_files_removed[1]),len(rst.users_whose_files_removed[2]),len(rst.users_whose_files_removed[3]),
                    len(rst.users_whose_files_protected[0]),len(rst.users_whose_files_protected[1]),len(rst.users_whose_files_protected[2]),len(rst.users_whose_files_protected[3])))
                    
                    with open(self.output_root+'/'+self.date_str+'_user_'+p+'_'+str(plen)+'_rank_'+str(self.rank)+'.csv', 'wt') as user_out:
                        all_user_ids = sorted(self.userIDMap[i].keys())
                        user_out.write('userID,job_activeness,pub_activeness,num_purged_files,num_retained_files,size_purged_files,size_retained_files\n')
                        for uid in all_user_ids:
                            uzr = self.userIDMap[i].get(uid)
                            if uzr:
                                user_out.write("{},{},{},{},{},{},{}\n".format(uzr.userID, uzr.job_activeness, uzr.pub_activeness,
                                uzr.num_purged_files[p][i],uzr.num_retained_files[p][i],uzr.size_purged_files[p][i],uzr.size_retained_files[p][i]))

    # guess file size in GB  https://www.olcf.ornl.gov/wp-content/uploads/2016/01/Best-Practices-v6.pdf
    def guess_file_size(self, OST):
        # default : <= 1TB across 4 stripes
        stripe_size = self.file_size_rand.randrange(1, 1024/4)
        if isinstance(OST, str):
            stripe_num = OST.count('|')+1
            if stripe_num == 512: # > 50TB, use 512
                return self.file_size_rand.randrange(50, 100) * 1024
            elif stripe_num > 4 and stripe_num < 512:  # 1-50 TB, use size/100GB as stripe count
                return stripe_num * 100
            elif stripe_num == 4: # default : <= 1TB across 4 stripes
                return stripe_num * stripe_size
            else:
                return stripe_size/4
        else:
            return stripe_size/4

    def _on_row(self, row):
        self.total_num_files+=1
        file_size = self.guess_file_size(row.OST)
        self.total_file_size+=file_size

        time_list = [('Atime',row.Atime), ('Ctime',row.Ctime), ('Mtime',row.Mtime)]
        time_list.sort(reverse=True, key=lambda t:t[1])
        self.user_ids_scanned_in_spider_trace.add(row.Uid)

        self.run_fixed_lifetime_policy(time_list, row, file_size)
        self.run_ares_policy(time_list, row, file_size)

    # dtype={"Atime":int,"Ctime":int,"Mtime":int,
    #         "OST":str,"area":str,"gid":str,"itemsize":int,
    #         "PATH":str,"permission":str,"project":str,"Uid":int,"unknown":str,"snapshot_date":str},
    def load_single_gzipped_csv(self, filepath):
        csv_reader = CSVReader(filepath, self._on_row, sep=',', 
            header=None, compression='gzip', names=["Atime","Ctime","Mtime","OST","area","gid","itemsize",
            "PATH","permission","project","Uid","unknown","snapshot_date"], 
            usecols=["Atime","Ctime","Mtime", "OST", "PATH","Uid"])
        df = csv_reader.load_csv()
        csv_reader.iter_csv_rows()
        del [[df]]
        gc.collect()

    def __run_sim(self):
        start_time_1 = time.time()
        self.output_activeness()

        start_time_2 = time.time()

        dir_path = self.spider_trace_root_path+"/"+self.date_str
        listOfGzipCSVs = sorted([f for f in os.listdir(dir_path) if fnmatch.fnmatch(f, "part-*.csv.gz")])
        total_count = len(listOfGzipCSVs)
        count = 0
        for f in listOfGzipCSVs:
            if count % self.size == self.rank:
                print("Rank {} start processing {}".format(self.rank, self.date_str+"/"+f))
                file_start_time = time.time()
                self.load_single_gzipped_csv(dir_path+"/"+f)
                file_end_time = time.time()
                print("Rank {} processed {} in {} s".format(self.rank, self.date_str+"/"+f, file_end_time-file_start_time))
            count += 1
            # if self.size == 1 and count >= 5: # for debugging purpose
            #     break
        start_time_3 = time.time()
        self.output_rst()
        end_time = time.time()
        print("{}: Activeness Evaluation: {} s, Scanning and Analyzing files {} s, Generating recommendation {} s, Overall {} s"
            .format(self.date_str, start_time_2-start_time_1, start_time_3-start_time_2, end_time-start_time_3, 
            end_time-start_time_1))


    def run(self):
        if self.function == 'sim':
            self.__run_sim()
        else:
            self.reduce_result()


    def __reduce_user_result(self, p, date_str):
        print("reducing user result")
        for plen in self.testing_periods:
            fn_pattern = date_str+'_user_'+p+'_'+str(plen)+'_rank*.csv'
            print(self.output_root+'/'+fn_pattern)
            rst_set = sorted([f for f in os.listdir(self.output_root) if fnmatch.fnmatch(f, date_str+'_user_'+p+'_'+str(plen)+'_rank*.csv')])
            if len(rst_set) > 0:
                rst_df_list = list()
                for f in rst_set:
                    rst_df_list.append(pd.read_csv(self.output_root+'/'+f, sep=',', header=0, compression=None))

                rst_df =reduce(lambda x, y: pd.concat([x, y]).groupby(['userID']).sum().reset_index(), rst_df_list)
                rst_df.insert(0, 'days', [plen]*rst_df.shape[0])
                rst_df.to_csv(self.output_root+'/'+date_str+'_user_'+p+'_'+str(plen)+'_reduced.csv', index=False)

    def __reduce_overall_result(self, date_str):
        for p in ['fixed', 'ares']:
            print("reducing overall result")
            fn_pattern = date_str+"_overall_"+p+"_rank*.csv"
            print(self.output_root+'/'+fn_pattern)
            rst_set = sorted([f for f in os.listdir(self.output_root) if fnmatch.fnmatch(f, fn_pattern)])
            if len(rst_set) > 0:
                rst_df_list = list()
                for f in rst_set:
                    rst_df_list.append(pd.read_csv(self.output_root+'/'+f, sep=',', header=0, compression=None))

                rst_df =reduce(lambda x, y: pd.concat([x, y]).groupby(['days']).sum().reset_index(), rst_df_list)
                rst_df.insert(0, 'policy', [p]*rst_df.shape[0])
                rst_df.to_csv(self.output_root+'/'+date_str+'_overall_'+p+'_reduced.csv', index=False)
                self.__reduce_user_result(p, date_str)
    

    def reduce_result(self):
        self.__reduce_overall_result(self.date_str)
