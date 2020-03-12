#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   program.py
@Time    :   2019/11/15 10:34:07
@Author  :   sui mingyang 
@Version :   0.0.1
@Contact :   suimingyang123@gmail.com
@License :   (C)Copyright 2018-2019, weidian
@Desc    :   None
'''

# here put the import lib
from base import conf
from pandas.tseries.offsets import Day
import time
from aliyun.log import GetLogsRequest
from dataserver import ossdataserver,mysqlserver
from orm import mysql_ORM
from aliyun.log import LogClient
import re
import pandas as pd
import datetime
import json
import math
from urllib import parse
from config import activityid_list,channelid_list,page_list
from config import start_time,end_time

def statistics(start_time,end_time):

    mysql_server = mysqlserver(conf.get('mysqldatabase','server'),int(conf.get('mysqldatabase','port')),conf.get('mysqldatabase','user'),conf.get('mysqldatabase','pwd'),conf.get('mysqldatabase','db'))
    oss_server = ossdataserver(conf.get('ossaccess','endpoint'),conf.get('ossaccess','accessKeyId'),conf.get('ossaccess','accessKey'),conf.get("ossdatabase",'basename'),conf.get("ossdatabase",'tablename'))
    
    # data_return_obj=pd.read_sql("""select date,user_id,return_num from (SELECT substr(create_time,1,7) as date,user_id,count(1) as return_num FROM weidian_operator.t_tj_order_detail_qingshe where create_time>'2019-10-20 00:00:00' and mp_alias='gh1b14f7568197' and order_status in ('维权结束','退签关闭','发货前取消 | 订单关闭','发货后取消 | 订单关闭') group by date,user_id) a where return_num>10;""",mysql_server)
    # # 统计用户pv数
    # sql_userpv="""* and key: UB_WXA_PAGE_VIEW and page:pages/productDetail/productDetail and gh1b14f7568197 | SELECT  date,userId , pv from (select substr(cast(from_unixtime(time/1000) as varchar),1,10)  as date,userId,count(1) as pv from log where userId is not null GROUP  by date,userId)  GROUP  by date,userId,pv  limit 1000000000"""
    # request_userpv = GetLogsRequest('weidian-fe-production','user-behavior' , fromTime=start_time, toTime=end_time, topic='', query=sql_userpv, line=100000000, offset=0, reverse=False)
    # res_userpv = oss_server.client.get_logs(request_userpv)

    # user_list=[]
    # pv_list=[]
    # print("用户数：",len(res_userpv.get_logs()))
    # for item in res_userpv.get_logs():
    #     user_list.append(item.get_contents()['userId'])
    #     pv_list.append(item.get_contents()['pv'])


    #u_list=data_return_obj['user_id']

    u_list=pd.read_csv('user.csv')[u'用户id']

    user_length=10
    res_all=[]
    u_list=[str(o) for o in u_list]
    for i in range(math.ceil(len(u_list)/user_length)):
        sql_user="""* and (key:UB_WXA_PAGE_LEAVE or key:UB_WXA_PAGE_VIEW) and page:pages/productDetail/productDetail | select substr(cast(from_unixtime(time/1000) as varchar),1,10)  as date,lid,time,key,page,netflow,options,referer,refererOptions,userId from log where userId in (%s) order by time asc limit 1000000000""" % (','.join(u_list[i*user_length:i*user_length+user_length-1]))
        #sql：userId
        #请求userId
        request_user = GetLogsRequest('weidian-fe-production','user-behavior' , fromTime=start_time, toTime=end_time, topic='', query=sql_user, line=100000000, offset=0, reverse=False)
        res_user = oss_server.client.get_logs(request_user)

        print("日志数：",len(res_user.get_logs()))
        res_all.append(res_user.get_logs())
    
    #统计分组计数
    dtdate=[]
    dttime=[]
    dtlid=[]
    dtkey=[]
    dtpage=[]
    dtoptions=[]
    dtnetflow=[]
    dtreferer=[]
    dtrefererOptions=[]
    dtuserId=[]

    print("日志数：",len(res_all))

    #日志数据转dataframe
    for i in res_all:
        for item in i:
            #print(item)
            dtdate.append(item.get_contents()['date'])
            dttime.append(item.get_contents()['time'])
            dtkey.append(item.get_contents()['key'])
            dtpage.append(item.get_contents()['page'])
            dtlid.append(item.get_contents()['lid'])
            dtoptions.append(item.get_contents()['options'])
            dtnetflow.append(item.get_contents()['netflow'])
            dtreferer.append(item.get_contents()['referer'])
            dtrefererOptions.append(item.get_contents()['refererOptions'])
            dtuserId.append(item.get_contents()['userId'])
    data_sql = {
        "date":pd.Series(dtdate),
        "time":pd.Series(dttime),
        "key":pd.Series(dtkey),
        "page":pd.Series(dtpage),
        "lid":pd.Series(dtlid),
        "netflow":pd.Series(dtnetflow),
        "options":pd.Series(dtoptions),
        "referer":pd.Series(dtreferer),
        "refererOptions":pd.Series(dtrefererOptions),
        "userId":pd.Series(dtuserId)
    }
    df = pd.DataFrame(data_sql,index=None)
    
    itemid_list={}
    for item in df.groupby(['userId']):
        start_obj={}
        item=item[1].sort_values('time', ascending=True)#正序排列
        keyline=""
        pv=0
        for i,temp in enumerate(item['page']):
            try:
                obj=item.loc[item['time'].index[i]]
                #开始时间
                if type(start_obj)==dict and temp in page_list and obj['key']=='"UB_WXA_PAGE_VIEW"': #and obj['netflow'] in channelid_list:
                    start_obj=obj
                    if 'options' in start_obj and 'id' in json.loads(start_obj['options']).keys() and json.loads(start_obj['options'])['id']!='null':
                        keyline=start_obj['date']+'_'+str(json.loads(start_obj['userId']))+'_'+start_obj['netflow']+'_'+start_obj['page']
                        pv=pv+1
                        continue
                    else:
                        start_obj={}
                        continue

                if type(start_obj)!=dict and obj['key']=='"UB_WXA_PAGE_LEAVE"' and temp==start_obj['page']: #and obj['netflow'] in channelid_list:
                    #有效计数
                    #获取itemid_list，存在计数，不存在加一
                    if keyline in list(itemid_list.keys()):
                        staytime = int(obj['time'])-int(start_obj['time'])
                        if staytime<0:
                            print(staytime)
                        itemid_list[keyline]["staytime"]+=int(staytime/1000)
                        itemid_list[keyline]["pv"]+=1
                        start_obj={}#置空
                    else:
                        itemid_list[keyline]={'pv':0,"staytime":0}
                        staytime = int(obj['time'])-int(start_obj['time'])
                        if staytime<0:
                            print(staytime)
                        itemid_list[keyline]["staytime"]+=int(staytime/1000)
                        itemid_list[keyline]["pv"]+=1
                        start_obj={}#置空
                else:
                    pass
            except Exception as e:
                print("error:",e)

    date_list=[]
    key_list=[]
    channel_list=[]
    pagename_list=[]
    #returnnum_list=[]
    staytime_list=[]
    pv_dict_list=[]

    for key in itemid_list.keys():
        try:
            pv_dict_list.append(itemid_list[key]['pv'])
            date_list.append(key.split('_')[0])
            key_list.append(key.split('_')[1])
            channel_list.append(key.split('_')[2].replace('"',''))
            pagename_list.append(key.split('_')[3].replace('"',''))
            #returnnum_list.append(itemid_list[key]['returnnum'])
            staytime_list.append(itemid_list[key]['staytime'])
        except Exception as e:
            print(e)
            continue

    result_data_sql = {
        'date':pd.Series(date_list),
        'user_id':pd.Series(key_list),
        'netflow':pd.Series(channel_list),
        #'exit_num':pd.Series(returnnum_list),
        'stay_time(s)':pd.Series(staytime_list),
        'pv':pd.Series(pv_dict_list)
    }

    result_df = pd.DataFrame(result_data_sql,index=None)

    result_df.to_csv(u"./csv/%s-deceiverise.csv" % start_time.split(':')[0],index=None)

if __name__ == "__main__":
    start_date='2019-11-01'
    end_date='2019-11-30'

    s_date=datetime.datetime.strptime(start_date,"%Y-%m-%d")
    e_date=datetime.datetime.strptime(end_date,"%Y-%m-%d")

    for i,t in enumerate(range((e_date-s_date).days+1)):
        curr=str(s_date+datetime.timedelta(days=t)).split(' ')[0]
        print(curr)
        statistics('{0} 00:00:00'.format(curr),'{0} 23:59:59'.format(curr))
        #static('{0} 18:00:00'.format(curr),'{0} 22:00:00'.format(curr))











