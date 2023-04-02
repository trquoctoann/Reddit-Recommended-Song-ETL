FROM trquoctoann/hadoop
LABEL maintainer = quoctoan09072002@gmail.com

RUN wget https://downloads.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz
RUN tar -xzf apache-hive-2.3.9-bin.tar.gz
RUN mv apache-hive-2.3.9-bin /usr/local/hive
RUN rm apache-hive-2.3.9-bin.tar.gz

ENV HIVE_HOME /usr/local/hive
ENV PATH $HIVE_HOME/bin:$PATH

RUN echo "HADOOP_HOME=/usr/local/hadoop" >> $HIVE_HOME/bin/hive-config.sh

RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.32.tar.gz
RUN tar -xzf mysql-connector-j-8.0.32.tar.gz
RUN cp mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar $HIVE_HOME/lib/
RUN rm mysql-connector-j-8.0.32.tar.gz

ADD config/hive-site.xml $HIVE_HOME/conf/

EXPOSE 10000 10002