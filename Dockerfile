FROM apache/spark-py:v3.4.0

USER root

COPY . /app
WORKDIR /app

RUN pip3 install -r requirements.txt

ENTRYPOINT ["spark-submit", "/app/main/app.py"]