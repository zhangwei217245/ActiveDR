import hashlib, re, copy
import os
import psutil
import os.path
from os import path
import random
from random import Random


from lib.data_source.ornl.UserActivityAnalyzer import *
from datetime import datetime, date

class FileSystemTrie(object):
    def __init__(self, encode=False):
        self.root = dict()
        self.encode = encode
        self.num_keys=0
        self.uname_set=set()
        self.capacity=0

    def __try_hash_code(self, st):
        return hashlib.sha1(st.encode('utf-8')).hexdigest() if self.encode else st

    def __insert_in_trie(self, path_array, node, dt):
        if len(path_array) == 0:
            node['|date']=dt
            self.uname_set.add(dt[1])
        else:
            d=self.__try_hash_code(path_array.pop(0))
            if node.get(d) is None:
                node[d]=dict()
            self.__insert_in_trie(path_array, node[d], dt)

    def insert_in_trie(self, path_array, dt):
        self.__insert_in_trie(path_array, self.root, dt)
        self.num_keys+=dt[2]
    
    def __search_in_trie(self, path_array, node, rst, dt):
        if len(path_array)==0:
            if node.get('|date') is None: # no |date key, miss
                rst.append(None)
            else: # has key, return result and update access date
                rst.append(node.get('|date'))
            return
        else:
            d=self.__try_hash_code(path_array.pop(0))
            # print(d, node.get(d))
            if node.get(d) is None:
                rst.append(None)
                return
            else:
                self.__search_in_trie(path_array, node[d], rst, dt)

    def search_in_trie(self, path_array, dt):
        rst=[]
        self.__search_in_trie(path_array, self.root, rst, dt)
        return rst[0]

    def __traverse_all_nodes(self, func, args, stack, visited, node, k, level):
        if k=="|date":
            func(node, k, stack, visited, level, args);
        else:
            if k not in visited:
                visited.append(k+str(level))
                if len(args)==3 and isinstance(args[2], User):
                    if len(stack)>0 and stack[-1]=='scratch' and k!=args[2].username:
                        return
                stack.append(k)
                for n in node[k].keys():
                    self.__traverse_all_nodes(func, args, stack, visited, node[k], n, level+1)
                stack.pop()
                visited.pop()

    def traverse_all_nodes(self, func, args):
        stack=list()
        visited=list()
        node = self.root
        level = 0
        for k in node.keys():
            self.__traverse_all_nodes(func, args, stack, visited, node, k, level+1)

    def set_capacity(self, capa):
        self.capacity = capa if self.capacity <= 0 else self.capacity

    def get_path_name_hash(self, pathname):
        e = [ self.__try_hash_code(e) \
                for e in pathname.split('/')[1:]]
        return "/" + "/".join(e)


class Fixed_Purge_Policy(object):
    def __init__(self, activity_trace_loader, lifetime=90, purge_interval=7):
        self.lifetime=lifetime
        self.purge_interval=purge_interval
        self.activity_trace_loader = activity_trace_loader
        self.activity_data = activity_trace_loader.load_activity_data()
        self.userNameMap=self.activity_data[0]
        self.userNodeMap=self.activity_data[1]
        self.pubNodeMap=self.activity_data[2]
        self.uaAnalyzer=None
        self.userIDMap=None
        self.num_purged =0
        self.file_map = [0,0,0,0]
        

    def do_analysis(self, purge_param):
        curr_dt = purge_param[0]
        purge_limit = purge_param[1]
        fs_trie = purge_param[2]
        dampen_factor=purge_param[3]
        self.uaAnalyzer = UserActivityAnalyzer(0, 1, self.activity_data, curr_dt.timestamp(), self.lifetime) if self.uaAnalyzer is None else self.uaAnalyzer
        analysis_result = self.uaAnalyzer.run()
        self.userIDMap=analysis_result[0] if self.userIDMap is None else self.userIDMap

    def clear_analysis(self):
        self.uaAnalyzer = None
        self.userIDMap = None
        rst_file_map=copy.deepcopy(self.file_map)
        total_purged = self.num_purged
        for i in range(4):
            self.file_map[i]=0
        self.num_purged=0
        return (total_purged, rst_file_map)

    def purge_action(self, node, k, stack, visited, level, purge_param):
        # print("/"+"/".join([ x for x in stack]), node[k][0].date(), node[k][1])
        if self.num_purged >= purge_param[1]:
            return
        if node is not None and node.get(k) is not None and node[k][0] is not None:
            if (purge_param[0] - node[k][0]).days > self.lifetime*purge_param[3]:
                self.num_purged+=node[k][2]
                node[k]=None
            else:
                u = self.userNameMap.get(node[k][1])
                if u is not None:
                    user = self.userIDMap.get(u.userID)
                    if user.is_both_active():
                        self.file_map[3]+=node[k][2]
                    if user.is_job_active_only():
                        self.file_map[2]+=node[k][2]
                    if user.is_pub_active_only():
                        self.file_map[1]+=node[k][2]
                    if user.is_both_inactive():
                        self.file_map[0]+=node[k][2]
                # else:
                    # self.file_map[3]+=node[k][2]

class ActiveDR(object):
    def __init__(self, activity_trace_loader, lifetime=90, purge_interval=7):
        self.lifetime=lifetime
        self.purge_interval=purge_interval
        self.activity_trace_loader = activity_trace_loader
        self.activity_data = activity_trace_loader.load_activity_data()
        self.userNameMap=self.activity_data[0]
        self.userNodeMap=self.activity_data[1]
        self.pubNodeMap=self.activity_data[2]
        self.uaAnalyzer=None
        self.userIDMap=None
        self.num_purged =0
        self.file_map = [0,0,0,0]

    def do_analysis(self, purge_param):
        curr_dt = purge_param[0]
        purge_limit = purge_param[1]
        fs_trie = purge_param[2]
        dampen_factor=purge_param[3]
        self.uaAnalyzer = UserActivityAnalyzer(0, 1, self.activity_data, curr_dt.timestamp(), self.lifetime) if self.uaAnalyzer is None else self.uaAnalyzer
        analysis_result = self.uaAnalyzer.run()
        self.userIDMap=analysis_result[0] if self.userIDMap is None else self.userIDMap
        sorted_users = analysis_result[1]
        lifetime_shrink=1.0
        for gi in range(len(sorted_users)):
            lifetime_shrink=1.0
            while self.num_purged < purge_limit:
                for u in sorted_users[gi]:
                    if u.username in fs_trie.uname_set:
                        fs_trie.traverse_all_nodes(self.delete_file_by_user, [curr_dt, purge_limit, u, lifetime_shrink])
                if lifetime_shrink<=0.0:
                    print("give up retry user group {}".format(gi))
                    break
                else:
                    lifetime_shrink-=0.5
                    print("retry user group {}".format(gi))

    def clear_analysis(self):
        self.uaAnalyzer = None
        self.userIDMap = None
        rst_file_map=copy.deepcopy(self.file_map)
        total_purged = self.num_purged
        for i in range(4):
            self.file_map[i]=0
        self.num_purged=0
        return (total_purged, rst_file_map)

    def delete_file_by_user(self, node, k, stack, visited, level, purge_param):
        if self.num_purged >= purge_param[1]:
            return
        user = purge_param[2]
        if node is not None and node.get(k) is not None and node[k][0] is not None:
            if user.username == node[k][1]:
                if purge_param[0].timestamp() - node[k][0].timestamp() > self.uaAnalyzer.time_spec.job_period_len*user.get_lifetime_coefficient()*purge_param[3]:
                    self.num_purged+=node[k][2]
                    node[k]=None
                else:
                    if user.is_both_active():
                        self.file_map[3]+=node[k][2]
                    if user.is_job_active_only():
                        self.file_map[2]+=node[k][2]
                    if user.is_pub_active_only():
                        self.file_map[1]+=node[k][2]
                    if user.is_both_inactive():
                        self.file_map[0]+=node[k][2]
    
    def purge_action(self, node, k, stack, visited, level, purge_param):
        return
                        
class LightweightPurgeSimulator(object):
    def __init__(self, purge_policy, logbase=""):
        self.applog_base = logbase
        self.output_dir = logbase+"/output"
        self.purge_date_file = ""
        self.trie=dict()
        self.years=[ 2015, 2016 ]
        self.months = [ x for x in range(1, 13) ]
        self.days= [ x for x in range(1,32) ]
        self.purge_policy= purge_policy
        self.curr_dt=None
        self.fs_trie = FileSystemTrie()
        self.file_size_rand = Random(x=5)
        self.miss_count_array=dict()
        self.miss_map=[0,0,0,0]
        self.userNameMap=None
        self.userIDMap=None
        self.important_miss_ratios=[1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        for i in range(len(self.important_miss_ratios)-1):
            self.miss_count_array[i]=0

    def guess_file_size(self, OST):
        # default : <= 1TB across 4 stripes
        stripe_size = self.file_size_rand.randrange(1, 256)
        if isinstance(OST, str):
            stripe_num = len(OST) #OST.count('|')+1
            if stripe_num == 512: # > 50TB, use 512
                return self.file_size_rand.randrange(50, 100) * 1024
            elif stripe_num > 4 and stripe_num < 512:  # 1-50 TB, use size/100GB as stripe count
                return stripe_num * self.file_size_rand.randrange(50, 100)
            elif stripe_num == 4: # default : <= 1TB across 4 stripes
                return stripe_num * stripe_size
            else:
                return stripe_size/4
        else:
            return stripe_size/4

    def extract_uname(self, path_array, rst):
        for i in range(len(path_array)):
            if path_array[i]=='scratch':
                if i == len(path_array)-1:
                    rst.append("unknown_user")
                    return "unknown_user"
                else:
                    rst.append(path_array[i+1])
                    return path_array[i+1]
        return None

    def index_one_trace(self, filepath):
        with open(filepath, encoding='latin-1') as fp:
            for line in fp:
                cnt = 1
                if line.find('scratch') >0:
                    if line.find('home')>0 or line.find('tmp/scratch')>0 or line.endswith('scratch'):
                        continue
                    dt = datetime.strptime(line.split(';')[3], '%Y-%m-%d %H:%M:%S')
                    rfd = re.findall(r'( \/\S*scratch\S* )',line)
                    rfd_arr = [ x.strip().split('/')[1:] for x in rfd ]
                    for path_array in rfd_arr:
                        # print("PATH  |  ",path_array)
                        uname=self.extract_uname(path_array,[])
                        input_buf = copy.deepcopy(path_array)
                        fsize = self.guess_file_size('|'.join(input_buf))
                        self.fs_trie.insert_in_trie(input_buf, (dt,uname,fsize))
                    
    def simulate_access(self, filepath):
        fp_accessed = 0
        fp_missed = 0
        with open(filepath, encoding='latin-1') as fp:
            for line in fp:
                cnt = 1
                if line.find('scratch') >0:
                    if line.find('home')>0 or line.find('tmp/scratch')>0 or line.endswith('scratch'):
                        continue
                    dt = datetime.strptime(line.split(';')[3], '%Y-%m-%d %H:%M:%S')
                    rfd = re.findall(r'( \/\S*scratch\S* )',line)
                    rfd_arr = [ x.strip().split('/')[1:] for x in rfd ]
                    for path_array in rfd_arr:
                        uname=self.extract_uname(path_array,[])
                        input_buf = copy.deepcopy(path_array)
                        fsize = self.guess_file_size('|'.join(input_buf))
                        found = self.fs_trie.search_in_trie(input_buf, (dt, uname, fsize))
                        fp_accessed+=1
                        if not found:
                            # print("NOTFOUND |",path_array)
                            fp_missed+=1
                            if self.userNameMap is not None and self.userIDMap is not None:
                                u = self.userNameMap.get(uname)
                                if u is not None:
                                    user=self.userIDMap[u.userID]
                                    if user.is_both_active():
                                        self.miss_map[3]+=1
                                    if user.is_job_active_only():
                                        self.miss_map[2]+=1
                                    if user.is_pub_active_only():
                                        self.miss_map[1]+=1
                                    if user.is_both_inactive():
                                        self.miss_map[0]+=1
                        input_buf = copy.deepcopy(path_array)
                        self.fs_trie.insert_in_trie(input_buf, (dt,uname, fsize))
            file_miss_ratio = fp_missed/(fp_accessed if fp_accessed>0 else 1)*100
            print("{} file_miss: {:.3f}%".format(self.curr_dt, file_miss_ratio), self.miss_map)
            for i in range(len(self.important_miss_ratios)-1):
                lower_bound = self.important_miss_ratios[i] 
                upper_bound = self.important_miss_ratios[i+1]
                if file_miss_ratio >= lower_bound and file_miss_ratio < upper_bound:
                    self.miss_count_array[i]+=1


    def purge_file_sys(self):
        purge_limit = self.fs_trie.capacity*50/100
        purge_param=[self.curr_dt, purge_limit, self.fs_trie, 1.0]
        self.purge_policy.do_analysis(purge_param)
        while True:
            self.fs_trie.traverse_all_nodes(self.purge_policy.purge_action, purge_param)
            if self.purge_policy.num_purged >= purge_limit:
                break
            else:
                if purge_param[3] <= 0.0:
                    break
                # purge_param[3]-=0.2
                
        self.fs_trie.num_keys-=self.purge_policy.num_purged
        self.userNameMap = self.purge_policy.userNameMap
        self.userIDMap = self.purge_policy.userIDMap
        return self.purge_policy.clear_analysis()

    def simulate_2_year(self):
        num_days = 0
        num_purge = 0
        for y in self.years:
            for m in self.months:
                for d in self.days:
                    filepath = self.applog_base+"/{}/applog/{:02}/{}{:02}{:02}-insert.txt".format(y,m,y,m,d)
                    if path.exists(filepath):
                        if y==2015:
                            self.index_one_trace(filepath)
                        else:
                            num_days+=1
                            if num_days % self.purge_policy.purge_interval == 4:
                                if num_purge==0:
                                    self.fs_trie.set_capacity(self.fs_trie.num_keys)
                                self.curr_dt = datetime(y,m,d, 23, 59, 59)
                                purge_rst = self.purge_file_sys()
                                print("PURGE_RST", self.curr_dt, purge_rst[0], purge_rst[1])
                                num_purge+=1
                            if num_purge <= 0:
                                self.index_one_trace(filepath)
                            else:
                                self.simulate_access(filepath)
