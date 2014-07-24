#!/usr/bin/env python
#-*- coding: utf-8 -*-
import json
import time
import sys
from common.cmdLib import *
import tempfile
import multiprocessing
from common.fileLib import *
import random
import datetime
MONITOR_INTERVAL=60
import threadpool

class taskrunner(object):
    def __init__(self,json_cfg):
        self.json_cfg=json_cfg
        self.sample_list=[]
        self.data_file_path=self.json_cfg['data_file_path']
    def rand_sample(self):
        random_sample_cmd_list=self.json_cfg['random_sample_cmd_list']
        sample_num=self.json_cfg['sample_num']
        tmp_file_list=[]
        process_list=[]
        for random_sample_cmd in random_sample_cmd_list:
            tmp_file=tempfile.mktemp()
            tmp_file_list.append(tmp_file)
            process_list.append(multiprocessing.Process(target=self.rand_cmd,args=(random_sample_cmd,tmp_file)))
        for process in process_list:
            process.start()
        for process in process_list:
            process.join()

        for tmp_file in tmp_file_list:
            lines=get_file_lines(tmp_file)
            for line in lines:
                line=line.strip()
                if line not in self.sample_list:
                    self.sample_list.append()

        self.sample_list=random.sample(self.sample_list,sample_num)
        print "sample_list:\n"
        print self.sample_list

        cu_time=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        fp=open(self.data_file_path+"sample_list"+"."+cu_time+".txt","w")
        for sample in self.sample_list:
            fp.write(sample+"\n")
        fp.close()

    def get_history_info(self):
        history_info_cmd_list=self.json_cfg['history_info_cmd_list']
        for sample in self.sample_list:
            if(sample != 'NULL'):
                tmp_file_list=[]
                process_list=[]
                sample_info_list=[]
                for history_info_cmd in history_info_cmd_list:
                    tmp_file=tempfile.mktemp()
                    tmp_file_list.append(tmp_file)
                    process_list.append(multiprocessing.Process(target=self.get_history_info_cmd,args=(history_info_cmd,sample,tmp_file)))
                for process in process_list:
                    process.start()
                for process in process_list:
                    process.join()
                for tmp_file in tmp_file_list:
                    lines=get_file_lines(tmp_file)
                    for line in lines:
                        line=line.strip()
                        sample_info_list.append(line)
                cu_time=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                fp=open(self.data_file_path+"sample_his_info_"+sample+"."+cu_time+".txt","w")
                for sample_info in sample_info_list:
                    fp.write(sample_info+"\n")
                fp.close()
    def download_ecommend_src(self):
        file_path_list=self.json_cfg['recommendinfo_monitor_cmd_list']
        self.general_monitor(file_path_list)
        recommendinfo_download_file_list=self.json_cfg['recommendinfo_download_file_list']
        download_thread_pool_size=self.json_cfg['download_thread_pool_size']
        pool = threadpool.ThreadPool(download_thread_pool_size)
        reqs = threadpool.makeRequests(self.download_recommend_src_cmd,recommendinfo_download_file_list,self.on_download_finish)
        [pool.putRequest(req) for req in reqs]
        pool.wait()
    def get_recommend_info(self):
        for sample in self.sample_list:
            if(sample != 'NULL'):
                cu_time=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                cmd_str="cat * | grep "+sample+" >> "+self.data_file_path+"sample_rec_info_"+sample+"."+cu_time+".txt"

    def rand_cmd(self,cmd_str,tmp_file):
        cmdstr="hive -S -e \""+cmd_str+"\" >> "+tmp_file
        print cmdstr
        status,output=cmd_execute(cmdstr)
    def get_history_info_cmd(self,cmd_str,cuid,tmp_file):
        cmd_str="hive --hiveconf hive.fetch.task.conversion=more -e \""+cmd_str.replace("{cuid}",cuid)+"\" >> "+tmp_file
        print cmd_str
        status,output=cmd_execute(cmdstr)
    def download_recommend_src_cmd(self,file_path):
        download_data_file_path=self.json_cfg['download_data_file_path']
        cmdstr="hadoop fs -get "+file_path+" "+download_data_file_path
        print cmdstr
        status,output=cmd_execute(cmdstr)
        return status,output
    def on_download_finish(self,request,n):
        print n
    def general_monitor(self,file_path_list):
        while 1:
            ready_flag_list=[]
            for file_path in file_path_list:
                cmd_str=file_path
                print cmd_str
                ready_flag_list.append(cmd_execute(cmd_str)[0])
            tmp_list=[0 for i in range(len(file_path_list))]
            print tmp_list
            print ready_flag_list
            if(tmp_list == ready_flag_list):
                print("hadoop文件ready，开始启动下载任务，并且等待任务结束...")
                break
            else:
                print("hadoop文件还未ready，"+str(MONITOR_INTERVAL)+"s之后继续监测...")
                time.sleep(MONITOR_INTERVAL)



