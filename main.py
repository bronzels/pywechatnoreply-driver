import os
import time
import logging
from pyspark.sql import SparkSession

from libpycommon.common.misc import get_env

from data.wechatmessagelist_classifier import entry,mylogger

BOOL_DEBUG_SWITCH = get_env('DEBUG_SWITCH', 'off') == 'on'
if __name__ == "__main__":
    # .master("local[*]")\
    if BOOL_DEBUG_SWITCH:
        ss = SparkSession \
            .builder \
            .appName("pywechatnoreply") \
            .config('spark.sql.execution.arrow.maxRecordsPerBatch', '20000') \
            .config("spark.sql.execution.arrow.pyspark.enabled", 'true') \
            .getOrCreate()
    else:
        ss = SparkSession \
            .builder \
            .appName("pywechatnoreply") \
            .config('spark.sql.execution.arrow.maxRecordsPerBatch', '20000') \
            .config("spark.sql.execution.arrow.pyspark.enabled", 'true') \
            .config("spark.jars.ivy", "/tmp/.ivy") \
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .getOrCreate()
    #export SPARK_SUBMIT_OPTS="--illegal-access=permit -Dio.netty.tryReflectionSetAccessible=true "
    # .config("spark.kubernetes.namespace", "batchpy") \
    # .config("spark.kubernetes.authenticate.driver.serviceAccountName", "pycmclient-admin") \
    s=time.time()
    entry(ss)
    ss.stop()
    mylogger.info(f'duration@{time.time() - s}')

    """
    
    import time
    from libpycommon.common.misc import get_env, get_env_encrypted
    from mykey.me import package_key_res_path
    envs = {
        'DB_HOST': '192.168.74.44',
        'DB_DATABASE': 'AcadsocWechatManager',
        'DB_USER': 'akBRhW1FrkFEYudsDRdT8Bj8qJIZhkgTJQaBH0nFw4l75BGoZ+ev8fUheKEr74y1Ji8pYYKQ6FBZsye/WhFEI8EUV+QiN9E0z5r/C6cx7ltlomD3znXb2rNfJQIgyX95QyOvdQbwLe4cRDWD0H7ZmjucmJ+c4amvGrafTqrqwic=',
        'DB_PASSWD': 'f9hIGzVnv7BokwRNgK14iLFB+b5dLXG1Z8Xmh6dVPPLPB+UnrmnJCL2qGS0ssf+n4b4S8QmMuK8g6hDxSS1GLmK3CSN99xWk1Wm5d1tpBuWO6xscQM3zBf66oZPI8UH4LcA6rHAFVNQzGyN+eRbk1T8WnilwV+9MTf3a8p34aww=',
        'WRITE_MODE': 'API',
        'GRPC_HOST': '10.15.67.2',
        'PYSPARK_PYTHON': r'D:/pyprojs/venvs/pywechatnoreply-driver/Scripts/python',
        'DEBUG_SWITCH': 'on',
        'GRPC_PORT': '30001',
        'API_HOST': '192.168.74.54',
        'API_PORT': '7007',

    }
    for k in envs:
        print((k+":"+os.environ.get(k,'')))

    print(package_key_res_path)
    print('DB_USER:'+get_env_encrypted('DB_USER', package_key_res_path))
    print('DB_PASSWD:'+get_env_encrypted('DB_PASSWD', package_key_res_path))

    while 1:
        time.sleep(300)



    """
