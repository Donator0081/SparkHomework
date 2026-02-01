FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    python3-pip \
    python3-dev \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir pyspark==3.5.0 pandas numpy
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONUNBUFFERED=1
WORKDIR /app