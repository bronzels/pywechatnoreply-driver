import grpc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, pandas_udf, PandasUDFType, lit, spark_partition_id

from proto import helloworld_pb2
from proto import helloworld_pb2_grpc

from pyspark import SparkContext,SparkConf
ss = SparkSession \
    .builder \
    .appName("pyspark-tensorflow-test") \
    .config('spark.sql.execution.arrow.maxRecordsPerBatch', '100000') \
    .config("spark.sql.execution.arrow.pyspark.enabled", 'true') \
    .getOrCreate()



df = ss.range(0, 256).withColumn('id', (col('id') / 10000).cast('integer')).withColumn('v', rand())
#df.repartition(2)

df.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()

#df.cache()
#print(df.count())
df.show(500)

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

# 输入和输出都是 doubles 类型的 pandas.Series
@pandas_udf('string', PandasUDFType.SCALAR)
def pandas_plus_one(v):
    send_mail('今天吃鸡吗'+str(len(v)))
    channel = grpc.insecure_channel('10.15.67.2:30002')
    stub = helloworld_pb2_grpc.GreeterStub(channel)
    response = stub.SayHello(helloworld_pb2.HelloRequest(name='len-'+str(len(v))))
    print("Greeter client received: " + response.message)
    return v.map(lambda x: response.message+str(x**2))
    #return v + 1

df=df.withColumn('v2', pandas_plus_one(df.v))
df=df.withColumn('cst', lit(0))

df.show(500)

