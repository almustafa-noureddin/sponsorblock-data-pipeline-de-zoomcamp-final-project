FROM apache/airflow:2.2.4

ENV AIRFLOW_HOME=/opt/airflow
ENV DBT_PROFILES_DIR: /opt/airflow/.dbt

USER root
RUN apt-get update -qq && apt-get install zstd -qqq
RUN /usr/local/bin/python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
#RUN sudo apt install zstd
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID