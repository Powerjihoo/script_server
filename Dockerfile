FROM tensorflow/tensorflow:2.15.0-gpu

# Install build tools and Python 3.11
RUN apt-get update && \
    apt-get install -y python3 python3-pip build-essential

# Set the locale to avoid potential issues with Python UTF-8 encoding
ENV LANG C.UTF-8

# Install protobuf 3.18.1
RUN apt-get install -y wget && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.18.1/protobuf-all-3.18.1.tar.gz && \
    tar -xzvf protobuf-all-3.18.1.tar.gz && \
    cd protobuf-3.18.1 && \
    ./configure && \
    make && \
    make install && \
    ldconfig

# Add /usr/local/bin to the PATH environment variable for protobuf
ENV PATH="${PATH}:/usr/local/bin/"

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

WORKDIR /realtime-prediction-server
COPY ./src ./src
COPY ./static ./static
COPY ./realtime-prediction-server.yaml ./realtime-prediction-server.yaml

ENTRYPOINT ["tail", "-f", "/dev/null"]