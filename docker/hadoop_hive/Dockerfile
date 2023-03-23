FROM ubuntu
LABEL maintainer = quoctoan09072002@gmail.com
RUN apt update && apt install -y openssh-server openssh-client vim openjdk-8-jdk

#SSH
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN sed -ri 's/#   StrictHostKeyChecking ask/    StrictHostKeyChecking accept-new/g' /etc/ssh/ssh_config
RUN chmod 0600 ~/.ssh/authorized_keys

RUN echo "root:123" | chpasswd
RUN echo "root   ALL=(ALL)       ALL" >> /etc/sudoers

#HADOOP
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
RUN tar -xzf hadoop-3.3.4.tar.gz
RUN mv hadoop-3.3.4 /usr/local/hadoop
RUN rm hadoop-3.3.4.tar.gz

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_CONFIG_DIRECTORY $HADOOP_HOME/etc/hadoop
ENV PATH $HADOOP_HOME/sbin:$PATH
ENV PATH $HADOOP_HOME/bin:$PATH

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# RUN mkdir /home/hadoop
# RUN mkdir /home/hadoop/tmp /home/hadoop/hdfs_name /home/hadoop/hdfs_data

ADD config/*.xml $HADOOP_CONFIG_DIRECTORY
ADD config/slaves $HADOOP_CONFIG_DIRECTORY
ADD config/*.sh $HADOOP_HOME/sbin


#Hive
RUN wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
RUN tar -xzf apache-hive-3.1.3-bin.tar.gz
RUN mv apache-hive-3.1.3-bin /usr/local/hive
ENV HIVE_HOME /usr/local/hive
ENV PATH $HIVE_HOME/bin:$PATH

RUN echo "HADOOP_HOME=/usr/local/hadoop" >> $HIVE_HOME/bin/hive-config.sh