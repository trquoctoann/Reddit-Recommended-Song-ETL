FROM ubuntu
LABEL maintainer = quoctoan09072002@gmail.com

RUN apt update && apt install -y wget openjdk-8-jdk
RUN apt update && apt install -y git scala python3

RUN wget https://archive.apache.org/dist/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz
RUN tar -xzf spark-3.0.3-bin-hadoop2.7.tgz
RUN mv spark-3.0.3-bin-hadoop2.7 usr/local/spark
RUN rm spark-3.0.3-bin-hadoop2.7.tgz

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH
ENV SPARK_HOME /usr/local/spark
ENV PATH $SPARK_HOME/bin:$PATH
ENV PATH $SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON usr/bin/python3

EXPOSE 4040 6066 7077 8080 8081