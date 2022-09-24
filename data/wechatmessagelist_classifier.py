import os
import time
import grpc
import datetime
import logging
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import split, floor as _floor
from pyspark.sql.types import *
from pyspark.sql.functions import udf

from proto.data_pb2 import *
from proto.data_pb2_grpc import ClassifyDataStub

from libpycommon.inoutput.sparkdf import get_by_sql_cols, set_df_overwrite, set_df_append, set_df_truncate
from libpycommon.inoutput.constant import *
from libpycommon.common.misc import get_env, get_env_encrypted
from libpycommon.common import mylog

from mykey.me import package_key_res_path,package_key_abs_path
from data.data_dump import yes_no_map,to_api

mylogger=mylog.get_logger()


def udfwarp(HOST, PORT):
    def compute_single_output(id,text,HOST,PORT):
        conn = grpc.insecure_channel(HOST + ':' + PORT)
        client = ClassifyDataStub(channel=conn)
        a=[InputRecord(id=str(i),text=j) for i,j in zip([id], [text])]#+[InputRecord(id='-999',text='{"text":"#"}$|${"text":"#"}')]
        b = Input(records=a)
        output = client.Do(b)
        out_list=[outputRecord.classifier * 1000 + outputRecord.rawclassifier for outputRecord in output.records]# if outputRecord.id!="-999"
        conn.close()
        return out_list[0]
    return udf(lambda x,y:compute_single_output(x,y,HOST,PORT))



def entry(ss,sep="$|$",part_num=18):
    d_db = {
        jdbc_connection_keyname_host: get_env('DB_HOST'),
        jdbc_connection_keyname_port: 1433,
        jdbc_connection_keyname_user: get_env_encrypted('DB_USER', package_key_res_path),  #
        jdbc_connection_keyname_password: get_env_encrypted('DB_PASSWD', package_key_res_path),  #
        jdbc_connection_keyname_catalog: get_env('DB_DATABASE')
    }

    _MODE = get_env('WRITE_MODE')
    _HOST = get_env('GRPC_HOST')
    _PORT = get_env('GRPC_PORT')
    _API_HOST = get_env('API_HOST')
    _API_PORT = get_env('API_PORT')
    #需要找到目标df,要改
    start_time_str = str(datetime.datetime.now()-datetime.timedelta(hours=2))[:-12]+'00:00.000'
    end_time_str=str(datetime.datetime.now()-datetime.timedelta(hours=1))[:-12]+'00:00.000'
    SQL=f"SELECT Id,Id as WechatNoReplyAnalysisInfoId,IsNoReply2, " \
        f"CONCAT(LastContentsAllSupport,'{sep}',LastContentsALL) AS text " \
        f"FROM WechatNoReplyAnalysisInfoByHour WHERE CreateTime > '{start_time_str}' and CreateTime < '{end_time_str}' and IsNoReply2 = 1"

    #测试用SQL
    #SQL="SELECT Id,Id as WechatNoReplyAnalysisInfoId,IsNoReply2, CONCAT(LastContentsAllSupport,'$|$',LastContentsALL) AS text FROM WechatNoReplyAnalysisInfoByHour WHERE IsNoReply2 = 1 and CreateTime > '2020-05-30 11:11:49.000'"
    # SQL = f"SELECT TOP 6 Id,Id as WechatNoReplyAnalysisInfoId,IsNoReply2, " \
    #       f"CONCAT(LastContentsAllSupport,'{sep}',LastContentsALL) AS text " \
    #       f"FROM WechatNoReplyAnalysisInfoByHour WHERE CreateTime > '{start_time_str}' and CreateTime < '{end_time_str}' and IsNoReply2 = 1"


    df = get_by_sql_cols(ss, jdbc_alchemy_mssql, jdbc_driver_name_mssql, d_db,
                         SQL,
                         'Id', part_num)

    #df = df.persist(StorageLevel(False, True, False, False))
    # df.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()
    # exit(0)

    cnt=df.count()
    mylogger.info(f'df.count:{cnt}')

    #df = df.withColumn('mergeid', udfwarp(df.WechatNoReplyAnalysisInfoId,df.text))
    df = df.withColumn('mergeid', udfwarp(_HOST,_PORT)(df.WechatNoReplyAnalysisInfoId,df.text))
    df = df.withColumn("RawLabelsId", df.mergeid%1000)\
        .withColumn("IsNeedReplyLabelsId", _floor(df.mergeid/1000))

    CT=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
    mapdf=ss.createDataFrame(data=[{'IsNeedReplyLabelsId':k,
                                    'IsNeedReply':v,
                                    'CreateTime':CT,
                                    } for k,v in yes_no_map.items()])
    df =df.join(mapdf,['IsNeedReplyLabelsId'],'left').na.fill(-1)

    if _MODE=='API':
        df = df.drop('CreateTime','IsNoReply2','text','mergeid')
        mylogger.info('RDDcalculating')
        pandas_df = df.toPandas()
        pandas_df=pandas_df.astype('int')
        mylogger.info('RDDcalculated')
        data_list=pandas_df.values.tolist()
        #log.log(logging.INFO, msg=f'sample{str(data_list[0])}@'+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        #log.log(logging.INFO, msg=f'apiuri:{_API_HOST}:{_API_PORT}@'+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        res=to_api(data_list,_API_HOST,_API_PORT)

        if res:
            mylogger.info('data has dump')
            mylogger.info('res:{}'.format(res))
        else:
            mylogger.info('res:{}'.format(res))
    else:
        df = df.drop('IsNoReply2', 'text', 'mergeid')
        df.show(10)
        # set_df_overwrite(jdbc_alchemy_mssql, jdbc_driver_name_mssql, d_db, df, 'WechatMessageList_filtered_30days')
        set_df_append(jdbc_alchemy_mssql, jdbc_driver_name_mssql, d_db, df, 'WechatMessageNoReplyAlgorithm')
        # set_df_truncate(jdbc_alchemy_mssql, jdbc_driver_name_mssql, d_db, df, 'WechatMessageList_filtered_30days')

if __name__ == '__main__':
    s=time.time()
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, rand, pandas_udf, PandasUDFType
    part_num = 6
    sep = "$|$"
    ss = SparkSession \
        .builder \
        .appName("pyspark-tensorflow-test") \
        .config('spark.sql.execution.arrow.maxRecordsPerBatch', '100000') \
        .config("spark.sql.execution.arrow.pyspark.enabled", 'true') \
        .config("spark.executor.instances", '6') \
        .getOrCreate()

    entry(ss, sep="$|$", part_num=18)
    # import time
    # while 1:
    #     if datetime.datetime.now().strftime('%M')=='10':
    #         entry(ss, sep="$|$", part_num=6)
    #         print(datetime.datetime.now().strftime('%H:%I:%M'))
    #     else:
    #         time.sleep(30)

    """
    def send_mail(text):
        import smtplib
        from email.mime.text import MIMEText
        # 设置服务器所需信息
        # 163邮箱服务器地址
        mail_host = 'smtp.163.com'
        # 163用户名
        mail_user = 'bronzels@163.com'
        # 密码(部分邮箱为授权码)
        mail_pass = 'XJOLNHXIPTYILTGK'
        # 邮件发送方邮箱地址
        sender = 'bronzels@163.com'
        # 邮件接受方邮箱地址，注意需要[]包裹，这意味着你可以写多个邮件地址群发
        receivers = ['alexliu@acadsoc.com']
    
        # 设置email信息
        # 邮件内容设置
        message = MIMEText('content', 'plain', 'utf-8')
        # 邮件主题
        message['Subject'] = text
        # 发送方信息
        message['From'] = sender
        # 接受方信息
        message['To'] = receivers[0]
    
        # 登录并发送邮件
        try:
            smtpObj = smtplib.SMTP_SSL(mail_host)
            smtpObj.set_debuglevel(1)
            #smtpObj.ehlo(mail_host)
            # 登录到服务器
            smtpObj.login(mail_user, mail_pass)
            # 发送
            smtpObj.sendmail(
                sender, receivers, message.as_string())
            # 退出
            smtpObj.quit()
            print('success')
        except smtplib.SMTPException as e:
            print('error', e)  # 打印错误
    
    @pandas_udf(IntegerType(), PandasUDFType.SCALAR)
    def compute_output(id_series, text_series):
        # send_mail(f'part数量{len(id_series)}')
        # return pd.Series([101101 for _ in [str(id) for id in id_series]])
        conn = grpc.insecure_channel(_HOST + ':' + _PORT)
        client = ClassifyDataStub(channel=conn)
        a=[InputRecord(id=str(i),text=j) for i,j in zip(id_series, text_series)]+[InputRecord(id='-999',text='{"text":"1"}$|${"text":"2"}')]
        if len(a)==1:#这里判断不出0比较奇怪
            conn.close()
            return pd.Series([])
        #a=[InputRecord(id=str(i),text=j) for i,j in zip(id_series, text_series)]
        else:
            b=Input(records=a)
            output = client.Do(b)
            outputdict={outputRecord.id:outputRecord.classifier*1000+outputRecord.rawclassifier for outputRecord in output.records if outputRecord.id!="-999"}
            conn.close()
            return pd.Series([outputdict[_] for _ in [str(id) for id in id_series]])
    
    
    
    def _compute_output(id_series, text_series):
        conn = grpc.insecure_channel(_HOST + ':' + _PORT)
        client = ClassifyDataStub(channel=conn)
        a=[InputRecord(id=str(i),text=j) for i,j in zip(id_series, text_series)]+[InputRecord(id='-999',text='{"text":"1"}$|${"text":"2"}')]
        if len(a)==1:#这里判断不出0比较奇怪
            conn.close()
            return pd.Series([])
        #a=[InputRecord(id=str(i),text=j) for i,j in zip(id_series, text_series)]
        else:
            b=Input(records=a)
            output = client.Do(b)
            outputdict={outputRecord.id:outputRecord.classifier*1000+outputRecord.rawclassifier for outputRecord in output.records if outputRecord.id!="-999"}
            conn.close()
            return pd.Series([outputdict[_] for _ in [str(id) for id in id_series]])
    
    @pandas_udf(IntegerType(), PandasUDFType.SCALAR)
    def counterpartnum(id_series):
        return pd.Series([id*1001 for id in range(len(id_series))])
    
    
    rawint= udf(lambda x: x,returnType=IntegerType())
    
    """


