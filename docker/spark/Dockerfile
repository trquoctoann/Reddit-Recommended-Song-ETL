FROM bde2020/spark-master:3.2.1-hadoop3.2

USER root

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org -r requirements.txt

USER 1001
https://downloads.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz