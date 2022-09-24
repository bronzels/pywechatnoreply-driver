docker login harbor.my.org:1080

docker rmi harbor.my.org:1080/base/py/spark
docker pull harbor.my.org:1080/base/py/spark

docker build ./ --add-host pypi.my.org:192.168.0.62 -t harbor.my.org:1080/python-app/pywechatnoreply-driver
docker push harbor.my.org:1080/python-app/pywechatnoreply-driver

docker run --rm --name pywechatnoreply-driver -e PYTHONUNBUFFERED=1 -e DB_HOST=192.168.0.85 -e DB_DATABASE=AcadsocDataAnalysisAlgorithm -e DB_USER=iKAXkZ1EQJuyh5ergFG+zoX2JoTGlmEDP62oUP/GQ2ZCLw/K+6oYB/uH3bnrxYWXWIUKJa2UQwnBcTBeOUtdL8tsoyJhPJkjTcQXoq/DNpFgq17bv0DaklAKfHI3lId7tG2TD6BRwlYNL6oWGQbYSzSVpR+1+U5lR/GnW6BSeck= -e DB_PASSWD=RQvxaZ8aDlJc4+n/R0R+mM9pi6jrD18o9HrH4sRG6K9NRqS26eykOPa+C8/QZGHeoCH3R9lXe5ZZSw1FhC8CQlUKaJiJDuP5XrJcSc5Kj+etBixiHdQsqFaer4joAdqJ+0t2p8Ku3M93/AO+4SUks2Kxmj3LPWRlWGPeFgS5yYA= -e GRPC_HOST=10.15.67.2 -e GRPC_PORT=30001 -e PYSPARK_PYTHON=python3 harbor.my.org:1080/python-app/pywechatnoreply-driver:20210603194145_687 bash
docker stop pywechatnoreply-driver
docker rm pywechatnoreply-driver
