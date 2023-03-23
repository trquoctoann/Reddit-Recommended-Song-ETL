FROM apache/airflow:2.3.0

USER root

RUN apt update && apt-get install -y openjdk-11-jdk && apt-get install -y ant && apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org --no-cache-dir -r requirements.txt