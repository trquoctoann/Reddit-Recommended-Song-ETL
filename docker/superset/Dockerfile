FROM apache/superset

USER root
RUN apt-get update && apt-get install -y libsasl2-dev

USER superset

ENV SUPERSET_CONFIG /etc/superset/superset_config.py

RUN echo "SQLALCHEMY_DATABASE_URI = 'hive://hiveserver2:10000/default'" >> $SUPERSET_CONFIG

EXPOSE 8088

ENTRYPOINT ["superset", "run", "-p", "8088"]