FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install git -y && \
    apt-get install wget -y  

RUN wget "https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz" && \
    tar -xvzf spark-3.0.1-bin-hadoop2.7.tgz && \
    rm spark-3.0.1-bin-hadoop2.7.tgz && \
    mv spark-3.0.1-bin-hadoop2.7 /opt/spark

RUN apt-get install -y python3-pip python3-dev && \
    pip3 install --upgrade pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install --upgrade pip 

COPY . /app
RUN pip install -r /app/requirements.txt 

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR /app
CMD /opt/spark/bin/spark-submit app.py