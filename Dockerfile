FROM ubuntu:16.04

ARG PYTHON_VERSION=3.6

RUN apt-get update && \
    apt-get install -y wget bzip2 build-essential openjdk-8-jdk ssh sudo && \
    apt-get clean

# Add ubuntu user and enable password-less sudo
RUN useradd -mU -s /bin/bash -G sudo ubuntu && \
    echo "ubuntu ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Set up SSH. Docker will cache this layer. So the keys end up the same on all containers.
# We use a non-default location to simulate MLR, which doesn't have passwordless SSH.
# We still keep the private key for manual SSH during debugging.
RUN ssh-keygen -q -f ~/.ssh/docker -N "" && \
    cp ~/.ssh/docker.pub ~/.ssh/authorized_keys

# Install Spark and update env variables.
ENV SCALA_VERSION 2.11.8
ENV SPARK_VERSION 2.4.0
ENV SPARK_BUILD "spark-${SPARK_VERSION}-bin-hadoop2.7"
ENV SPARK_BUILD_URL "https://dist.apache.org/repos/dist/release/spark/spark-2.4.0/${SPARK_BUILD}.tgz"
RUN wget --quiet $SPARK_BUILD_URL -O /tmp/spark.tgz && \
    tar -C /opt -xf /tmp/spark.tgz && \
    mv /opt/$SPARK_BUILD /opt/spark && \
    rm /tmp/spark.tgz
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH
ENV PYTHONPATH /opt/spark/python/lib/py4j-0.10.7-src.zip:/opt/spark/python/lib/pyspark.zip:$PYTHONPATH
ENV PYSPARK_PYTHON python

# Set env variables for tests.
ENV RUN_ONLY_LIGHT_TESTS True

# The sparkdl dir will be mmounted here.
VOLUME /mnt/graphframes
WORKDIR /mnt/graphframes

ENTRYPOINT service ssh restart && /bin/bash
