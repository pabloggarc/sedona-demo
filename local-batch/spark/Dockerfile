# The apache/spark image comes with Python 3.10, too old for current geopandas and pyarrow, and
# apache/sedona is still on Spark 4.0. Spark 4.1 from PyPI on a plain Java image covers both.
FROM eclipse-temurin:17-jre-noble

ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv curl r-base-core \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

ENV PATH=/opt/venv/bin:$PATH \
    SPARK_HOME=/opt/venv/lib/python3.12/site-packages/pyspark \
    SPARK_CONF_DIR=/opt/spark/conf \
    PYTHONPATH=/home/spark/jobs/python

RUN cd $SPARK_HOME/jars \
 && curl -fsSL --remote-name-all \
      ${MAVEN}/org/apache/sedona/sedona-spark-shaded-4.1_2.13/1.9.1/sedona-spark-shaded-4.1_2.13-1.9.1.jar \
      ${MAVEN}/org/datasyslab/geotools-wrapper/1.9.1-33.5/geotools-wrapper-1.9.1-33.5.jar \
      ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
      ${MAVEN}/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

# Binary R packages from a dated snapshot of Posit's CRAN mirror, so nothing gets compiled
RUN Rscript -e ' \
      options(HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(), \
        paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"]))); \
      install.packages(c("sparklyr", "dplyr", "dbplyr"), \
        repos = "https://packagemanager.posit.co/cran/__linux__/noble/2026-10-15")'

COPY spark-defaults.conf log4j2.properties $SPARK_CONF_DIR/

RUN useradd --create-home --uid 185 spark
USER spark
WORKDIR /home/spark

CMD ["pyspark"]
