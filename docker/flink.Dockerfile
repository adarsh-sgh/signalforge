# PyFlink runtime for the anomaly job. Also the test image: apache-flink ships
# x86_64-only Linux wheels, so on an arm64 laptop this runs under emulation.
FROM flink:2.1.3-scala_2.12-java17

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python

COPY requirements-flink.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements-flink.txt

WORKDIR /opt/signalforge
ENV PYTHONPATH=/opt/signalforge
