#!/usr/bin/env python
#-*- coding: utf-8 -*-
import json
import time
import sys
from common.cmdLib import *
MONITOR_INTERVAL=60
from taskrunner import *

def main(cfg_file_path):
    while 1:
        json_cfg=json.load(open(cfg_file_path, "r"),encoding='utf-8')
        src_file_monitor_cmd_list=json_cfg['src_file_monitor_cmd_list']
        ready_flag_list=[]
        for src_file_monitor_cmd in src_file_monitor_cmd_list:
            cmd_str=src_file_monitor_cmd
            print cmd_str
            ready_flag_list.append(cmd_execute(cmd_str)[0])
        tmp_list=[0 for i in range(len(src_file_monitor_cmd_list))]
        print tmp_list
        print ready_flag_list
        if(ready_flag_list == tmp_list):
            print("hadoop文件ready，开始启动任务，并且等待任务结束...")
            tr=taskrunner(json_cfg)
            tr.rand_sample()
            tr.get_history_info()
            tr.get_recommend_info()
            #time.sleep(5)
            print("任务结束，退出监测...")
            return
        else:
            print("hadoop文件还未ready，"+str(MONITOR_INTERVAL)+"s之后继续监测...")
            time.sleep(MONITOR_INTERVAL)

if __name__ == '__main__':
    cfg_file_path=sys.argv[1]
    main(cfg_file_path)

