#!/usr/bin/env python
#-*- coding: utf-8 -*-
from common.cmdLib import *
from common.logLib import *
from common.fileLib import *
from common.mysqlLib import *
from common.queryUserPreference import *
from common.sftpLib import *
from common.msgSend import *
import datetime
import time
#随机抽样使用多进程加速
import multiprocessing
import os
import re
import random
import tempfile
#使用第三方线程池模块，执行hive查询操作
import threadpool
#后台执行编码异常问题
import sys
reload(sys)
sys.setdefaultencoding('utf8')

class class_map_category(object):
    def __init__(self,event_day):
        self.SAMPLENUM="50"
        self.SAMPLENUM_LOW="10"
        self.SAMPLENUM_HIGH="40"
        self.FREQUENCY="10"
        self.data_directory="./data/map_category_random/"
        self.sample_filepath="_sample_file"
        self.sample_list=[]
        self.history_info_list=[]
        self.history_info_filepath="_history_info_file"
        self.recommend_info_list=[]
        self.recommend_info_filepath="_recommend_info_file"
        self.pool_size=5
        self.event_day=event_day
        #self.latest_date=latest_date
        self.upps_src_data_directory="./data/map_category_random/upps_src_data/"
        self.download_srcdata_poolsize=10
        self.src_data_info={}
        self.src_data_info_filepath="_src_data_info"
    #浏览POI详情页随机抽样
    def rand_sample_browse_rawdata_low(self):
        outputfile=self.data_directory+"low_browse_rawdata_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select id from lbs_upp_user_browse_rawdata_trans_cuid group by id having count(*)<10 order by rand() limit """+self.SAMPLENUM_LOW+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_browse_rawdata_low:\n")
        print status,output
    def rand_sample_browse_rawdata_high(self):
        outputfile=self.data_directory+"high_browse_rawdata_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select id from lbs_upp_user_browse_rawdata_trans_cuid group by id having count(*)>10 order by rand() limit """+self.SAMPLENUM_HIGH+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_browse_rawdata_high:\n")
        print status,output
    #泛检索行为随机抽样
    def rand_sample_poi_search_cat1_low(self):
        outputfile=self.data_directory+"low_poi_search_cat1_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select user_id from upp_poi_profile where type='poi_search_cat1' and day="""+self.event_day+""" group by user_id having count(*)<10 order by rand() limit """+self.SAMPLENUM_LOW+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_poi_search_cat1_low:\n")
        print status,output
    def rand_sample_poi_search_cat1_high(self):
        outputfile=self.data_directory+"high_poi_search_cat1_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select user_id from upp_poi_profile where type='poi_search_cat1' and day="""+self.event_day+""" group by user_id having count(*)>10 order by rand() limit """+self.SAMPLENUM_HIGH+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_poi_search_cat1_high:\n")
        print status,output
    #停留点数据随机抽样
    def rand_sample_traj_rawdata_low(self):
        outputfile=self.data_directory+"low_traj_rawdata_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select id from user_traj_stay_point_trans_cuid group by id having count(*)<10 order by rand() limit """+self.SAMPLENUM_LOW+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_traj_rawdata_low:\n")
        print status,output
    def rand_sample_traj_rawdata_high(self):
        outputfile=self.data_directory+"high_traj_rawdata_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select id from user_traj_stay_point_trans_cuid group by id having count(*)>10 order by rand() limit """+self.SAMPLENUM_HIGH+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_traj_rawdata_high:\n")
        print status,output
    #浏览团单详情页随机抽样
    def rand_sample_fanbu_tuangouliulan_low(self):
        outputfile=self.data_directory+"low_fanbu_tuangouliulan_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select user_id from upp_poi_profile where type='fanbu_tuangouliulan' and day="""+self.event_day+""" group by user_id having count(*)<10 order by rand() limit """+self.SAMPLENUM_LOW+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_fanbu_tuangouliulan_low:\n")
        print status,output
    def rand_sample_fanbu_tuangouliulan_high(self):
        outputfile=self.data_directory+"high_fanbu_tuangouliulan_rand_sample_"+self.event_day
        cmdstr="""hive -S -e "use upp;select user_id from upp_poi_profile where type='fanbu_tuangouliulan' and day="""+self.event_day+""" group by user_id having count(*)>10 order by rand() limit """+self.SAMPLENUM_HIGH+"""">"""+outputfile+""""""
        print(cmdstr)
        status,output=cmd_execute(cmdstr)
        print("rand_sample_fanbu_tuangouliulan_high:\n")
        print status,output

    def batch_rand_sample(self):
        now=datetime.datetime.now()
        print(u"地图类目用户偏好-随机抽样-开始："+str(now))
        process_list=[]
        args=()
        process_list.append(multiprocessing.Process(target=self.rand_sample_browse_rawdata_high,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_browse_rawdata_low,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_fanbu_tuangouliulan_high,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_fanbu_tuangouliulan_low,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_poi_search_cat1_high,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_poi_search_cat1_low,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_traj_rawdata_high,args=args))
        process_list.append(multiprocessing.Process(target=self.rand_sample_traj_rawdata_low,args=args))
        for process in process_list:
            process.start()
        for process in process_list:
            process.join()
        now=datetime.datetime.now()
        print(u"地图类目用户偏好-随机抽样-结束："+str(now))
    def secondary_filter(self):
        #低频CUID二次过滤
        file_reg=r"^low_.*"+"_"+self.event_day+"$"
        print file_reg
        file_list=os.listdir(self.data_directory)
        print file_list
        filter_file_list=[]
        for f in file_list:
            if(re.match(file_reg,f)):
                filter_file_list.append(f)
        print filter_file_list
        cuid_list=[]
        for f in filter_file_list:
            lines=get_file_lines(self.data_directory+f)
            for line in lines:
                if(line not in cuid_list):
                    cuid_list.append(line)
                else:
                    print(str(line)+" already exists")
        print(cuid_list)
        print(str(len(cuid_list)))
        self.sample_list=random.sample(cuid_list,int(self.SAMPLENUM_LOW))
        print(self.sample_list)
        print(str(len(self.sample_list)))


        #高频CUID二次过滤
        file_reg=r"^high_.*"+"_"+self.event_day+"$"
        print file_reg
        file_list=os.listdir(self.data_directory)
        print file_list
        filter_file_list=[]
        for f in file_list:
            if(re.match(file_reg,f)):
                filter_file_list.append(f)
        print filter_file_list
        cuid_list=[]
        for f in filter_file_list:
            lines=get_file_lines(self.data_directory+f)
            for line in lines:
                if(line not in cuid_list):
                    cuid_list.append(line)
                else:
                    print(str(line)+" already exists")
        print(cuid_list)
        print(str(len(cuid_list)))
        self.sample_list+=random.sample(cuid_list,int(self.SAMPLENUM_HIGH))
        print(self.sample_list)
        print(str(len(self.sample_list)))

        fp=open(self.data_directory+self.event_day+self.sample_filepath,"w")
        for sample in self.sample_list:
            fp.write(sample)
        fp.close()
    def get_history_cuid_info_browse_rawdata(self,cuid):
        tmp_filepath=tempfile.mktemp()
        cmdstr="""hive --hiveconf hive.fetch.task.conversion=more -e "use upp;select * from lbs_upp_user_browse_rawdata_trans_cuid where  id='"""+cuid+"""'">"""+tmp_filepath+""""""
        print(cmdstr)

        status,output=cmd_execute(cmdstr)
        print("get_history_cuid_info_browse_rawdata:\n")
        print status,output
        #return output
        lines=get_file_lines(tmp_filepath)
        '''for i,line in enumerate(lines):
            lines[i]="browse_rawdata\t"+lines[i]'''
        try:
            os.remove(tmp_filepath)
        except Exception as e:
            print(tmp_filepath+" delete fail.")
            print(str(e))
        finally:
            print lines
            return lines
    def get_history_cuid_info_poi_search_cat1(self,cuid):
        tmp_filepath=tempfile.mktemp()
        cmdstr="""hive --hiveconf hive.fetch.task.conversion=more -e "use upp;select * from upp_poi_profile where type='poi_search_cat1'  and day="""+self.event_day+"""  and user_id='"""+cuid+"""'">"""+tmp_filepath+""""""
        print(cmdstr)

        status,output=cmd_execute(cmdstr)
        print("get_history_cuid_info_poi_search_cat1:\n")
        print status,output
        #return output
        lines=get_file_lines(tmp_filepath)
        '''for i,line in enumerate(lines):
            lines[i]="browse_rawdata\t"+lines[i]'''
        try:
            os.remove(tmp_filepath)
        except Exception as e:
            print(tmp_filepath+" delete fail.")
            print(str(e))
        finally:
            print lines
            return lines
    def get_history_cuid_info_traj_rawdata(self,cuid):
        tmp_filepath=tempfile.mktemp()
        cmdstr="""hive --hiveconf hive.fetch.task.conversion=more -e "use upp;select * from user_traj_stay_point_trans_cuid where  id='"""+cuid+"""'">"""+tmp_filepath+""""""
        print(cmdstr)

        status,output=cmd_execute(cmdstr)
        print("get_history_cuid_info_traj_rawdata:\n")
        print status,output
        #return output
        lines=get_file_lines(tmp_filepath)
        '''for i,line in enumerate(lines):
            lines[i]="browse_rawdata\t"+lines[i]'''
        try:
            os.remove(tmp_filepath)
        except Exception as e:
            print(tmp_filepath+" delete fail.")
            print(str(e))
        finally:
            print lines
            return lines
    def get_history_cuid_info_fanbu_tuangouliulan(self,cuid):
        tmp_filepath=tempfile.mktemp()
        cmdstr="""hive --hiveconf hive.fetch.task.conversion=more -e "use upp;select * from upp_poi_profile where type='fanbu_tuangouliulan'  and day="""+self.event_day+""" and user_id='"""+cuid+"""'">"""+tmp_filepath+""""""
        print(cmdstr)

        status,output=cmd_execute(cmdstr)
        print("get_history_cuid_info_fanbu_tuangouliulan:\n")
        print status,output
        #return output
        lines=get_file_lines(tmp_filepath)
        '''for i,line in enumerate(lines):
            lines[i]="browse_rawdata\t"+lines[i]'''
        try:
            os.remove(tmp_filepath)
        except Exception as e:
            print(tmp_filepath+" delete fail.")
            print(str(e))
        finally:
            print lines
            return lines
    def on_get_result(self,request,n):
        for line in n:
            self.history_info_list.append(line)
    def batch_get_history_cuid_info(self):
        for i,sample in enumerate(self.sample_list):
            self.sample_list[i]=self.sample_list[i].strip()

        now=datetime.datetime.now()
        print(u"地图类目用户偏好-历史数据查询-browse_rawdata-开始："+str(now))
        pool = threadpool.ThreadPool(self.pool_size)
        reqs = threadpool.makeRequests(self.get_history_cuid_info_browse_rawdata,self.sample_list,self.on_get_result)
        [pool.putRequest(req) for req in reqs]
        pool.wait()

        reqs = threadpool.makeRequests(self.get_history_cuid_info_poi_search_cat1,self.sample_list,self.on_get_result)
        [pool.putRequest(req) for req in reqs]
        pool.wait()

        reqs = threadpool.makeRequests(self.get_history_cuid_info_traj_rawdata,self.sample_list,self.on_get_result)
        [pool.putRequest(req) for req in reqs]
        pool.wait()

        reqs = threadpool.makeRequests(self.get_history_cuid_info_fanbu_tuangouliulan,self.sample_list,self.on_get_result)
        [pool.putRequest(req) for req in reqs]
        pool.wait()
        now=datetime.datetime.now()
        print(u"地图类目用户偏好-历史数据查询-browse_rawdata-结束："+str(now))

    def batch_get_recommend_cuid_info(self,servicekey,secretkey,subservice,srcType):
        while 1:
            try:
                get_file('/home/map/importer/TgBrand0_import/status.txt',self.data_directory+"status.txt")
                status0=get_file_lines(self.data_directory+"status.txt")[0].strip()
                get_file('/home/map/importer/TgBrand1_import/status.txt',self.data_directory+"status.txt")
                status1=get_file_lines(self.data_directory+"status.txt")[0].strip()
                print status0,status1
                if(status0 == 'checking' and status1 == 'checking'):
                    print(u"数据导入redis ready，开始查询系统推荐信息...")
                    break
                else:
                    print(u"数据导入redis还未ready，60s之后继续监测...")
                    time.sleep(60)
            except Exception as e:
                print(str(e))

        for i,sample in enumerate(self.sample_list):
            self.sample_list[i]=self.sample_list[i].strip()
        fp=open(self.data_directory+self.event_day+self.recommend_info_filepath,"w")
        for cuid in self.sample_list:
            print cuid
            #ret=str(queryUserPreference(servicekey,secretkey,subservice,cuid,srcType))
            ret='————'
            ret=cuid+"\t"+ret
            self.recommend_info_list.append(ret)
            fp.write(ret+"\n")
        fp.close()

    def download_upps_src_data(self,filepath):
        cmdstr="hadoop fs -get "+filepath+" "+self.upps_src_data_directory
        print cmdstr
        status,output=cmd_execute(cmdstr)
        return status,output
    def on_download_finish(self,request,n):
        print n
    def batch_download_upps_src_data(self):
        try:
            cmdstr="rm "+self.upps_src_data_directory+"*"
            print(cmdstr)
            status,output=cmd_execute(cmdstr)
            print status,output
        except Exception as e:
            print(str(e))
        while 1:
            cmdstr="hadoop fs -test -e /app/lbs/lbs-stat/upp/data/mr/up/map/category/"+self.event_day+"/done"
            e=cmd_execute(cmdstr)[0]
            if(e == 0):
                print(u"hadoop文件ready，开始批量下载文件...")
                break
            else:
                print(u"hadoop文件还未ready，60s之后继续监测...")
                time.sleep(60)
        now=datetime.datetime.now()
        print(u"地图类目用户偏好-UPPS源数据下载-开始："+str(now))
        filepath_list=[]
        for i in xrange(10):
            filepath_list.append("/app/lbs/lbs-stat/upp/data/mr/up/map/category/"+self.event_day+"/data/part-*"+str(i))
        print(filepath_list)

        pool = threadpool.ThreadPool(self.download_srcdata_poolsize)
        reqs = threadpool.makeRequests(self.download_upps_src_data,filepath_list,self.on_download_finish)
        [pool.putRequest(req) for req in reqs]
        pool.wait()

        now=datetime.datetime.now()
        print(u"地图类目用户偏好-UPPS源数据下载-完成："+str(now))
    def get_src_data_info(self):
        file_list=os.listdir(self.upps_src_data_directory)
        for i,f in enumerate(file_list):
            file_list[i]=self.upps_src_data_directory+file_list[i]
        print file_list
        for i,sample in enumerate(self.sample_list):
            self.sample_list[i]=self.sample_list[i].strip()
        print self.sample_list
        for f in file_list:
            lines=get_file_lines(f)
            for line in lines:
                line = line.strip()
                line_list=re.split(r'\t',line.rstrip('\t'))
                a=''
                for l in line_list[1:]:
                    a+=l
                    a+='\t'
                if(line_list[0] in self.sample_list):
                    self.src_data_info[line_list[0]]=a
        print self.src_data_info
        fp=open(self.data_directory+self.event_day+self.src_data_info_filepath,"w")
        for cuid in self.src_data_info:
            fp.write(cuid+"\t"+str(self.src_data_info[cuid])+"\n")
        fp.close()

def map_category_taskrun(event_day):
    #任务时间
    print(u"任务时间为："+str(event_day))

    #任务初始化
    map_category=class_map_category(event_day)
    mysql=mysqlLib()
    now=datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")
    param=("map_category",now,0,event_day)
    n,last_id=mysql.add_task(param)
    print n,last_id
    mysql.close()

    #多进程随机取样
    map_category.batch_rand_sample()
    #取样二次过滤
    map_category.secondary_filter()
    print map_category.sample_list
    mysql=mysqlLib()
    for sample in map_category.sample_list:
        sample=sample.strip()
        param=(last_id,sample,0)
        mysql.add_sample(param)
    mysql.close()

    #线程池下发历史信息查询任务
    map_category.batch_get_history_cuid_info()
    print map_category.history_info_list,len(map_category.history_info_list)
    fp=open(map_category.data_directory+map_category.event_day+map_category.history_info_filepath,"w")
    for line in map_category.history_info_list:
        fp.write(line)
    fp.close()

    filepath=map_category.data_directory+map_category.event_day+map_category.history_info_filepath
    lines=get_file_lines(filepath)
    mysql=mysqlLib()
    for line in lines:
        line=line.strip()
        line_list=re.split(r'\t',line.rstrip('\t'))
        #print(str(len(line_list)))
        if(len(line_list) == 6):
            param=(last_id,line_list[0],line_list[1],line_list[2],line_list[3],line_list[4],'','','browse_rawdata',line_list[5],)
        if(len(line_list) == 7):
            param=(last_id,line_list[0],line_list[1],line_list[2],line_list[3],line_list[4],'','',line_list[5],line_list[6],)
        if(len(line_list) == 9):
            param=(last_id,line_list[0],line_list[1],line_list[2],line_list[3],line_list[4],'','','traj_rawdata',line_list[8],)
        mysql.add_history_info(param)
    mysql.close()
    '''mysql=mysqlLib()
    for line in map_category.history_info_list:
        line = line.strip()
        line_list=re.split(r'\t',line.rstrip('\t'))
        param=(last_id,line_list[0],line_list[1],line_list[2],line_list[3],line_list[4],line_list[5],line_list[6],line_list[7],line_list[8],)
        mysql.add_history_info(param)
    mysql.close()'''

    #构造线程池，源数据文件批量下载
    map_category.batch_download_upps_src_data()
    #源数据文件解析
    map_category.get_src_data_info()
    #推荐信息查询
    map_category.batch_get_recommend_cuid_info("tuangou","10af214253a015b8ffdfbac9f98077b4","userpreference",12)
    print map_category.recommend_info_list
    #推荐信息入库
    mysql=mysqlLib()
    for recommend_info in map_category.recommend_info_list:
        recommend_info_list=recommend_info.split("\t")
        param=(last_id,recommend_info_list[0],recommend_info_list[1])
        mysql.add_recommend_info(param)
    mysql.close()
    #源数据信息入库
    mysql=mysqlLib()
    for cuid in map_category.src_data_info:
        param=(str(map_category.src_data_info[cuid]),last_id,cuid)
        mysql.update_recommend_info(param)
    mysql.close()


    #任务结束
    mysql=mysqlLib()
    now=datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")
    param=(now,1,last_id)
    mysql.update_task_endtime_status(param)

    #任务结束，短信通知
    msgSend(['18665817689','15220056030','15019478061'],u'TASK FINISHED:map_category badcase mining.')

if __name__ == "__main__":
    map_category_taskrun("20140504")

