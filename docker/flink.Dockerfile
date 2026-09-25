# PyFlink runtime for the anomaly job, and the image the PyFlink tests run in:
# apache-flink publishes x86_64-only Linux wheels, so on an arm64 laptop this runs emulated
# (`make flink-test`). CI is x86_64 and installs the same requirements natively.
FROM flink:2.1.3-scala_2.12-java17

ARG KAFKA_CONNECTOR=5.0.0-2.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar"

COPY requirements-flink.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements-flink.txt

# the job adds this to the MiniCluster / job classpath itself (SF_FLINK_JARS)
ENV SF_FLINK_JARS=file:///opt/flink/lib/flink-sql-connector-kafka.jar
ENV PYTHONPATH=/opt/signalforge
WORKDIR /opt/signalforge
