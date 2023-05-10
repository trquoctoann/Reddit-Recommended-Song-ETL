FROM ubuntu
LABEL maintainer = quoctoan09072002@gmail.com
RUN apt update && apt install -y openssh-server openssh-client vim openjdk-8-jdk

#SSH
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN sed -ri 's/#   StrictHostKeyChecking ask/    StrictHostKeyChecking no/g' /etc/ssh/ssh_config
RUN chmod 0600 ~/.ssh/authorized_keys

RUN echo "root:123" | chpasswd
RUN echo "root   ALL=(ALL)       ALL" >> /etc/sudoers

#HADOOP
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz
RUN tar -xzf hadoop-2.7.7.tar.gz
RUN mv hadoop-2.7.7 /usr/local/hadoop
RUN rm hadoop-2.7.7.tar.gz

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $HADOOP_HOME/sbin:$PATH
ENV PATH $HADOOP_HOME/bin:$PATH
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV YARN_HOME $HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib/native
ENV HADOOP_OPTS "-Djava.library.path=$HADOOP_HOME/lib/native"

RUN echo "ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir /home/hadoop
RUN mkdir /home/hadoop/tmp /home/hadoop/hdfs_name /home/hadoop/hdfs_data

ADD config/*.xml $HADOOP_CONF_DIR
ADD config/workers $HADOOP_CONF_DIR
RUN chmod +x $HADOOP_HOME/sbin/*

RUN apt install -y net-tools
ARG FORMAT_NAMENODE_COMMAND
RUN $FORMAT_NAMENODE_COMMAND

EXPOSE 50070 8088 9000
EXPOSE 22