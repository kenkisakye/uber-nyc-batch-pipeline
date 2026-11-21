# PySpark jobs: monthly validation (and, from step 5, the quarterly release).
ARG SPARK_IMAGE=apache/spark:4.2.0-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

# S3A connector, resolved at build time for exactly the Hadoop version that
# ships with this Spark image (mismatched versions are a common failure).
# Since Hadoop 3.4.2, hadoop-aws declares the AWS SDK bundle and the S3
# analytics accelerator as "provided", so they are not downloaded
# automatically: their versions are read from hadoop-aws's own dependency
# list and fetched explicitly, so all three always match.
RUN set -eux; \
    hadoop_version="$(basename /opt/spark/jars/hadoop-client-api-*.jar .jar | sed 's/^hadoop-client-api-//')"; \
    echo 'print("resolved")' > /tmp/noop.py; \
    resolve() { /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages "$1" /tmp/noop.py; }; \
    resolve "org.apache.hadoop:hadoop-aws:${hadoop_version}"; \
    ivy_file="$(find /tmp/ivy -path '*hadoop-aws*' -name "ivy-${hadoop_version}.xml" | head -1)"; \
    dep_version() { grep "org=\"$1\"" "$ivy_file" | grep "name=\"$2\"" | sed -E 's/.*rev="([^"]+)".*/\1/' | head -1; }; \
    sdk_version="$(dep_version software.amazon.awssdk bundle)"; \
    accel_version="$(dep_version software.amazon.s3.analyticsaccelerator analyticsaccelerator-s3)"; \
    echo "Hadoop ${hadoop_version}, AWS SDK bundle ${sdk_version:-none}, analytics accelerator ${accel_version:-none}"; \
    packages="org.apache.hadoop:hadoop-aws:${hadoop_version}"; \
    [ -n "$sdk_version" ] && packages="${packages},software.amazon.awssdk:bundle:${sdk_version}"; \
    [ -n "$accel_version" ] && packages="${packages},software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:${accel_version}"; \
    resolve "$packages"; \
    cp /tmp/ivy/jars/org.apache.hadoop_hadoop-aws-*.jar /opt/spark/jars/; \
    cp /tmp/ivy/jars/software.amazon.awssdk_bundle-*.jar /opt/spark/jars/; \
    if ls /tmp/ivy/jars/software.amazon.s3.analyticsaccelerator_*.jar >/dev/null 2>&1; then \
    cp /tmp/ivy/jars/software.amazon.s3.analyticsaccelerator_*.jar /opt/spark/jars/; \
    fi; \
    ls -l /opt/spark/jars | grep -E 'hadoop-aws|bundle|analyticsaccelerator'; \
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
