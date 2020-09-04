FROM openjdk:8u242-jre

WORKDIR /opt

ENV METASTORE_VERSION=3.0.0
ENV HADOOP_VERSION=3.2.0
ENV HIVE_VERSION=3.1.2
ENV HIVE_METASTORE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
RUN apt-get update && apt-get install -y netcat
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}

RUN curl -L https://www-us.apache.org/dist/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zxf - && \
        curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf - && \
        wget https://jdbc.postgresql.org/download/postgresql-9.4.1212.jar -O $HIVE_METASTORE_HOME/lib/postgresql-jdbc.jar

# following were needed because of an issue while processing the CSV files
# https://community.hortonworks.com/content/supportkb/247055/errorjavalangunsupportedoperationexception-storage.html
# https://stackoverflow.com/questions/54035636/how-to-fix-get-table-schema-from-server-error-in-hive
#---------------------------------------------------
RUN wget https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/${HIVE_VERSION}/hive-metastore-${HIVE_VERSION}.jar
RUN mv hive-metastore-${HIVE_VERSION}.jar $HIVE_METASTORE_HOME/lib/hive-metastore-${HIVE_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hive/hive-common/${HIVE_VERSION}/hive-common-${HIVE_VERSION}.jar
RUN mv hive-common-${HIVE_VERSION}.jar $HIVE_METASTORE_HOME/lib/hive-common-${HIVE_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hive/hive-serde/${HIVE_VERSION}/hive-serde-${HIVE_VERSION}.jar
RUN mv hive-serde-${HIVE_VERSION}.jar $HIVE_METASTORE_HOME/lib/hive-serde-${HIVE_VERSION}.jar
#---------------------------------------------------

COPY entrypoint.sh /entrypoint.sh
COPY hive-site.xml $HIVE_METASTORE_HOME/conf

RUN chmod +x /entrypoint.sh

RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_METASTORE_HOME} hive && \
    chown hive:hive -R ${HIVE_METASTORE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

RUN ln -s $HIVE_METASTORE_HOME /opt/hive

USER hive
EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
