FROM apache/airflow:3.0.2

WORKDIR /opt
USER root
RUN mkdir -p /opt/data && chown -R airflow:0 /opt/data
USER airflow

COPY . .

USER airflow

RUN pip install poetry
RUN pip install -r requirements.in
RUN pip install .

WORKDIR /opt/airflow
