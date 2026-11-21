# PySpark jobs: monthly validation (and, from step 5, the quarterly release).
ARG SPARK_IMAGE=apache/spark:4.2.0-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

# S3A connector, resolved at build time for exactly the Hadoop version that
# ships with this Spark image (mismatched versions are a common failure).
RUN set -eux; \
    hadoop_version="$(basename /opt/spark/jars/hadoop-client-api-*.jar .jar | sed 's/^hadoop-client-api-//')"; \
    echo "Bundled Hadoop version: ${hadoop_version}"; \
    echo 'print("connector resolved")' > /tmp/noop.py; \
    /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy \
        --packages "org.apache.hadoop:hadoop-aws:${hadoop_version}" /tmp/noop.py; \
    cp /tmp/ivy/jars/org.apache.hadoop_hadoop-aws-*.jar \
       /tmp/ivy/jars/software.amazon.awssdk_*.jar /opt/spark/jars/; \
    rm -rf /tmp/ivy /tmp/noop.py /root/.ivy2*

COPY requirements.txt /app/requirements.txt
RUN PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --no-cache-dir \
        --disable-pip-version-check -r /app/requirements.txt

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs/ /app/jobs/
COPY tests/ /app/tests/
COPY run.sh /app/run.sh
RUN chmod 755 /app/run.sh

# Back to the image's unprivileged user
USER spark
WORKDIR /app
ENTRYPOINT ["/app/run.sh"]
