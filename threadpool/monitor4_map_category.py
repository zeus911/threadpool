#!/usr/bin/env python
#-*- coding: utf-8 -*-
from common.cmdLib import *
from common.logLib import *
from common.mysqlLib import *
from map_category import *
MONITOR_INTERVAL=60
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def get_monitor_task_date():
    mysql=mysqlLib()
    task_list=mysql.query_task({"offset":0,"row_cnt":1,"status":1,"type":'map_category'})
    mysql.close()
    latest_date=task_list[0][5]
    latest_date_time=datetime.datetime.strptime(latest_date,"%Y%m%d")
    print(u"最新任务时间:"+str(latest_date_time))
    #date=task_list[0][2].strftime('%Y%m%d')
    #date=task_list[0][2].date()
    monitor_date=latest_date_time+datetime.timedelta(days=1)
    print(u"检测任务时间:"+str(monitor_date))
    return latest_date,monitor_date.strftime('%Y%m%d')


if __name__ == "__main__":
    #print date
    while 1:
        latest_date,date=get_monitor_task_date()
        #date="20140413"
        print(u"检测任务日期:"+str(date))
        cmdstr1="""hadoop fs -test -e /app/lbs/lbs-stat/upp/data/hive/map/browse_rawdata/event_day="""+date+"""/done"""
        cmdstr2="""hadoop fs -test -e /app/lbs/lbs-stat/upp/data/hive/map/upp_poi_profile/type=poi_search_cat1/day="""+date+"""/done"""
        cmdstr3="""hadoop fs -test -e /app/lbs/lbs-stat/upp/data/hive/map/traj_rawdata/event_day="""+latest_date+"""/done"""
        cmdstr4="""hadoop fs -test -e /app/lbs/lbs-stat/upp/data/hive/map/upp_poi_profile/type=fanbu_tuangouliulan/day="""+date+"""/done"""
        e1=cmd_execute(cmdstr1)[0]
        e2=cmd_execute(cmdstr2)[0]
        e3=cmd_execute(cmdstr3)[0]
        e4=cmd_execute(cmdstr4)[0]
        print e1
        print e2
        print e3
        print e4
        if(e1==0 and e2 == 0 and e3 ==0 and e4 ==0):
            print("hadoop文件ready，开始启动任务，并且等待任务结束...")
            map_category_taskrun(date)
            print("任务结束，继续监测...")
        else:
            print("hadoop文件还未ready，"+str(MONITOR_INTERVAL)+"s之后继续监测...")
            time.sleep(MONITOR_INTERVAL)

