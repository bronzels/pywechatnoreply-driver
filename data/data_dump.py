# -*- coding: utf-8 -*-
import requests
import json
from libpycommon.common import mylog
mylogger=mylog.get_logger()
def to_db(df):pass
"""
data=[
{
"wechatNoReplyAnalysisInfoId": 12345,
"isNeedReply": 'true',
"isNeedReplyLabelsId": 0
},
{
    "wechatNoReplyAnalysisInfoId": 54321,
    "isNeedReply": 'true',
    "isNeedReplyLabelsId": 2
},
]
#'{"Body":{"Body":true,"ErrorCode":0,"Msg":"添加成功"},"ErrorCode":0,"Msg":""}'
#'{"Body":"","ErrorCode":2,"Msg":"Error converting value 1 to type \'Acadsoc.Sales.ServiceModel.WechatSystem.WechatNoReplyStatic.AddWechatMessageNoReplyAlgorithmSearchModel\'. Path \'[0]\', line 1, position 2.\\nError converting value 2 to type \'Acadsoc.Sales.ServiceModel.WechatSystem.WechatNoReplyStatic.AddWechatMessageNoReplyAlgorithmSearchModel\'. Path \'[1]\', line 1, position 5."}'

aheaders = {'Content-Type': 'application/json'}
res=requests.post(url,headers=aheaders,data=json.dumps(data))
"""
def to_api(li,host,port):
    data=[{
    "wechatNoReplyAnalysisInfoId": b,
    "isNeedReply": d,
    "isNeedReplyLabelsId": a,
    'RawLabelsId':c
    } for a,b,c,d in li]
    url=f'http://{host}:{port}/api/WechatSystem/AddWechatMessageNoReplyAlgorithmList'
        #'http://192.168.74.54:7007/api/WechatSystem/AddWechatMessageNoReplyAlgorithmList'
    headers = {'Content-Type': 'application/json'}
    res=requests.post(url,headers=headers,data=json.dumps(data))
    result=json.loads(res.content.decode('utf8'))
    mylogger.info(f'[result:{result}]')
    if result.get('ErrorCode',2)==0:
        mylogger.info('ErrorCode is 0')
        return 1
    else:
        return None



yes_no_map={
    0:1,
    1:1,
    2:1,
    3:1,
    4:1,
    5:1,
    6:1,
    7:1,
    8:1,
    9:1,
    10:1,
    11:0,
    12:0,
    13:0,
    14:0,
    15:0,
    16:0,
    17:0,
    18:0,
    19:0,
    101:0,
    103:0,
    104:0,
    105:0,
    106:0,
    201:0,
    202:0,
    102:1,
    107:1,
}
