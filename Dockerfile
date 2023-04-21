# Use an official Python runtime as a parent image
FROM apache/spark-py:v3.4.0

USER root

RUN apt-get update -y &&\
    apt-get install -y python3

COPY . /app
WORKDIR /app

RUN pip3 install -r requirements.txt


ENTRYPOINT ["spark-submit", "/app/main.py"]