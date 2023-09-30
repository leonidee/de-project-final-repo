FROM apache/airflow:2.7.1-python3.11

ENV APP_PATH=/opt/airflow

WORKDIR $APP_PATH
USER root

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl htop procps wget  \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY ./requirements.txt $APP_PATH/requirements.txt

RUN pip install --no-cache-dir --upgrade pip

# RUN pip install --no-cache-dir -r $APP_PATH/requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.1.5

RUN pip list
