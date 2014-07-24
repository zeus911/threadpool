#!/usr/bin/env python
#-*- coding: utf-8 -*-
from logLib import *
import datetime

import MySQLdb
host='db-rd-bu-09-da01.db01.baidu.com'
user='upp'
passwd='assessment'
db='db_badcase'
port=3306

'''host='cq01-rdqa-pool094.cq01.baidu.com'
user='root'
passwd='Admin@123'
db='db_badcase'
port=33306'''

from dynsqlLib import *

class mysqlLib():
    def __init__(self,host=host,user=user,passwd=passwd,db=db,port=port,charset='utf8'):
        try:
            self.host=host
            self.user=user
            self.passwd=passwd
            self.db=db
            self.port=port
            self.charset=charset
            self.conn=MySQLdb.connect(host=self.host,user=self.user,passwd=self.passwd,db=self.db,port=self.port,charset=self.charset)
            self.cursor=self.conn.cursor()
        except Exception as e:
            logging.error(str(e))
    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logging.error(str(e))
    ##################################
    def add_task(self,param):
        try:
            sql="""insert into tbl_task(id,type,starttime,status,event_day)
            values(NULL,%s,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def update_task_endtime_status(self,param):
        try:
            sql="""update tbl_task set endtime=%s,status=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def add_sample(self,param):
        try:
            sql="""insert into tbl_sample(id,task_id,cuid,is_badcase)
            values(NULL,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def update_sample_is_badcase(self,param):
        try:
            sql="""update tbl_sample set is_badcase=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def add_recommend_info(self,param):
        try:
            sql="""insert into tbl_recommend_info(id,task_id,cuid,recommend_info)
            values(NULL,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def update_recommend_info(self,param):
        try:
            sql="""update tbl_recommend_info set src_data_info=%s where task_id=%s and cuid=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def add_history_info(self,param):
        try:
            sql="""insert into tbl_tuandan_pinpai_history_info(id,task_id,t_day,user_id,pinpai_id,tag_info,user_id_type,tuandan_id,pinpai_name,type,day)
            values(NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0

    def query_task(self,param):
        try:
            s=DynSql("""select * from job where 1=1 and alg_type=2 { and job_id=$id} { and info=$task_id}
            { and sub_type=$type} { and start_time=$starttime} { and end_time=$endtime} { and status=$status} { and alg_day=$event_day}
             { and user=$submit_by} { and submit_time=$submit_time}
            ORDER BY job_id DESC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            print sql
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def query_task_totalnum(self):
        try:
            sql="""select * from job where alg_type=2"""
            cnt=self.cursor.execute(sql)
            cnt=self.cursor.rowcount
            return cnt
        except Exception as e:
            logging.error(str(e))
    def query_sample(self,param):
        try:
            s=DynSql("""select * from tbl_sample where 1=1 { and id=$id}
            { and task_id=$task_id} { and cuid=$cuid} { and is_badcase=$is_badcase}
            ORDER BY id DESC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def query_tuandan_pinpai_history_info(self,param):
        try:
            s=DynSql("""select * from tbl_tuandan_pinpai_history_info where 1=1 { and id=$id}
            { and task_id=$task_id} { and t_day=$t_day} { and user_id=$user_id} { and pinpai_id=$pinpai_id} { and tag_info=$tag_info}
            { and user_id_type=$user_id_type} { and tuandan_id=$tuandan_id} { and pinpai_name=$pinpai_name} { and type=$type} { and day=$day}
            ORDER BY t_day  {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def query_recommend_info(self,param):
        try:
            s=DynSql("""select * from tbl_recommend_info where 1=1 { and id=$id}
            { and task_id=$task_id} { and cuid=$cuid} { and recommend_info=$recommend_info} { and src_data_info=$src_data_info}
            ORDER BY id DESC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def query_total_task_management(self):
        try:
            sql="""select * from tbl_task_management"""
            cnt=self.cursor.execute(sql)
            cnt=self.cursor.rowcount
            return cnt
        except Exception as e:
            logging.error(str(e))

    def add_badcase(self,param):
        try:
            sql="""insert into tbl_badcase(id,task_id,type,marked_by,marked_time,info,task_day)
            values(NULL,%s,%s,%s,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def query_badcase(self,param):
        try:
            s=DynSql("""select * from tbl_badcase where 1=1 { and id=$id}
            { and task_id=$task_id} { and type=$type} { and marked_by=$marked_by} { and marked_time=$marked_time}
            { and info=$info} { and task_day=$task_day}
            ORDER BY id DESC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))







    def del_task_management(self,param):
        try:
            sql="delete from tbl_task_management where id=%s"
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def update_task_management(self,param):
        try:
            sql="""update tbl_task_management set pjt_id=%s,name=%s,version=%s,processtype=%s,testtype=%s,filepath=%s,info=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))

    def query_test_management(self,param):
        try:
            s=DynSql("""select * from tbl_test_management where 1=1 { and id=$id}
            { and task_id=$task_id} { and pjt_id=$pjt_id} { and testtype=$testtype} { and starttime=$starttime}
            { and endtime=$endtime} { and status=$status} { and log=$log} { and startby=$startby}
            ORDER BY id DESC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def query_test_management_monitor(self):
        try:
            sql="""SELECT * from tbl_test_management where status=0 ORDER BY id ASC LIMIT 0,1"""
            cnt=self.cursor.execute(sql)
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def query_total_test_management(self):
        try:
            sql="""select * from tbl_test_management"""
            cnt=self.cursor.execute(sql)
            cnt=self.cursor.rowcount
            return cnt
        except Exception as e:
            logging.error(str(e))
    def add_test_management(self,param):
        try:
            sql="""insert into tbl_test_management(id,task_id,pjt_id,testtype,starttime,status,startby)
            values(NULL,%s,%s,%s,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def update_test_management_endtime(self,param):
        try:
            sql="""update tbl_test_management set endtime=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def update_test_management_status(self,param):
        try:
            sql="""update tbl_test_management set status=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def update_test_management_log(self,param):
        try:
            sql="""update tbl_test_management set log=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def query_project_management(self,param):
        try:
            s=DynSql("""select * from tbl_project_management where 1=1 { and id=$id}
            { and name=$name} { and info=$info} { and status=$status} { and parent=$parent} { and position=$position}
            ORDER BY id ASC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def add_test_script(self,param):
        try:
            sql="""insert into tbl_script(id,task_id,caseid,casename,starttime)
            values(NULL,%s,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def update_test_script_endtime(self,param):
        try:
            sql="""update tbl_script set endtime=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def update_test_script_result(self,param):
        try:
            sql="""update tbl_script set result=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def update_test_script_testlog(self,param):
        try:
            sql="""update tbl_script set testlog=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def query_test_script(self,param):
        try:
            s=DynSql("""select * from tbl_script where 1=1 { and id=$id}
            { and task_id=$task_id} { and caseid=$caseid} { and casename=$casename} { and starttime=$starttime}
            { and endtime=$endtime} { and result=$result} { and testlog=$testlog}
            ORDER BY id ASC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def add_benchmark_test(self,param):
        try:
            sql="""insert into tbl_benchmark(id,task_id,casename,starttime)
            values(NULL,%s,%s,%s)"""
            n=self.cursor.execute(sql,param)
            last_id=int(self.cursor.lastrowid)
            self.conn.commit()
            return n,last_id
        except Exception as e:
            logging.error(str(e))
            return 0,0
    def update_benchmark_test(self,param):
        try:
            sql="""update tbl_benchmark set endtime=%s,totalrequest=%s,totalresponse=%s,totalerror=%s,qps_curve=%s where id=%s"""
            n=self.cursor.execute(sql,param)
            self.conn.commit()
            return n
        except Exception as e:
            logging.error(str(e))
    def query_benchmark_test(self,param):
        try:
            s=DynSql("""select * from tbl_benchmark where 1=1 { and id=$id}
            { and task_id=$task_id} { and casename=$casename} { and starttime=$starttime} { and endtime=$endtime}
            { and totalrequest=$totalrequest} { and totalresponse=$totalresponse} { and totalerror=$totalerror}
            ORDER BY id ASC {limit {$offset,} $row_cnt}""")
            sql=s(param)
            cnt=self.cursor.execute(sql[0],sql[1])
            ret=self.cursor.fetchall()
            return ret
        except Exception as e:
            logging.error(str(e))
    def add_task_submit(self,task_day,task_type,submit_by,now):
        try:
            sql="""select * from  tbl_task where event_day='"""+task_day+"""' and type='"""+task_type+"""';"""
            #print sql
            cnt=self.cursor.execute(sql)
            #print cnt
            ret=self.cursor.fetchall()
            #print ret
            if(cnt != 1):
                return u"数据源还未准备OK，请稍候查询"
            elif(cnt == 1):
                task_id=str(int(ret[0][0]))
                starttime=str(ret[0][2])
                endtime=str(ret[0][3])
                status=str(int(ret[0][4]))
                '''print task_id
                print starttime
                print endtime
                print  status'''
                sql="""insert into job(job_id,name,paths,alg_type,sub_type,info,start_time,end_time,status,user,alg_day,comment,result,sample_num)
                values(NULL,"""+"'job_name'"+""",'"""+"job_path"+"""','"""+"2"+"""','"""+task_type+"""',"""+task_id+""",'"""+starttime+"""','"""+endtime+"""','"""+status+"""','"""+submit_by+"""','"""+task_day+"""','"""+task_type+"""','"""+"3"+"""','"""+"50"+"""');"""
                print sql
                n=self.cursor.execute(sql)
                last_id=int(self.cursor.lastrowid)
                self.conn.commit()
                if(n != 1):
                    return u"任务提交失败，请联系系统管理员"
                if(n == 1):
                    return u"任务提交成功，您可以继续到badcase查询页面查看任务计算结果"

        except Exception as e:
            logging.error(str(e))
            return u"任务提交失败，请联系系统管理员"

if __name__ == "__main__":
    mysql=mysqlLib()

    '''n=mysql.add_pjt(param=("UserPreference",u"用户偏好",0))
    print(u"插入数据:"+str(n))

    ret=mysql.query_pjt()
    print(u"查询数据:"+str(len(ret)))
    print ret
    for record in ret:
        for field in record:
            print field

    n=mysql.updata_pjt(param=("UserPreference_NEW",u"用户偏好_NEW",1,"UserPreference"))
    print(u"更新数据:"+str(n))

    n=mysql.del_pjt(param=(u"UserPreference",))
    print(u"删除数据:"+str(n))'''

    '''param=(2,'task_userpreference','1.0.1.1',u'标记一下',0,1,
    'ftp://getprod:getprod@product.scm.baidu.com:/data/prod-64/app/search/lbs-da/upps/openservice/openservice_1-0-11-4_PD_BL',0)
    n,last_id=mysql.add_task(param)
    print(u"插入数据:"+str(n)+","+str(last_id))

    ret=mysql.query_task((0,10))
    print(u"查询数据:"+str(len(ret)))
    print ret
    for record in ret:
        for field in record:
            print field

    now=datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")
    param=(2,'task_userpreference_new','1.0.1.1',u'标记一下',0,1,
    'ftp://getprod:getprod@product.scm.baidu.com:/data/prod-64/app/search/lbs-da/upps/openservice/openservice_1-0-11-4_PD_BL',
    now,now,0,'task_userpreference')
    n=mysql.update_task(param)
    print(u"更新数据:"+str(n))

    ret=mysql.query_task((0,10))
    print(u"查询数据:"+str(len(ret)))
    print ret
    for record in ret:
        for field in record:
            print field

    n=mysql.del_task(param=(u"task_userpreference",))
    print(u"删除数据:"+str(n))'''

    ret=pjt_list=mysql.query_pjt({"offset":0,"row_cnt":10})
    print ret

    mysql.close()

