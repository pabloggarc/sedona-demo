# Spark 4.1 talks to the metastore with its built-in Hive 2.3 client, which works against a 3.x
# metastore but not a 4.x one. There is no arm64 image of Hive 3.1.3, so it runs emulated.

FROM apache/hive:3.1.3

ENV SERVICE_NAME=metastore \
    HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar

COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY entrypoint.sh /metastore-entrypoint.sh

ENTRYPOINT ["bash", "/metastore-entrypoint.sh"]
