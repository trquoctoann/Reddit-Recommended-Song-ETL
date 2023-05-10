FROM trquoctoann/hadoop
LABEL maintainer = quoctoan09072002@gmail.com

RUN apt update && apt install -y git scala python3

RUN wget https://archive.apache.org/dist/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz
RUN tar -xzf spark-3.0.3-bin-hadoop2.7.tgz
RUN mv spark-3.0.3-bin-hadoop2.7 usr/local/spark
RUN rm spark-3.0.3-bin-hadoop2.7.tgz

ENV SPARK_HOME /usr/local/spark
ENV PATH $SPARK_HOME/bin:$PATH
ENV PATH $SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON python3

ADD config/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
ADD config/hive-site.xml $SPARK_HOME/conf/

COPY requirements.txt ./requirements.txt
RUN apt-get update
RUN apt install -y python3-pip
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

EXPOSE 4040 6066 7077 8080 8081