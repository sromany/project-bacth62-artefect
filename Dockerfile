FROM apache/airflow:3.0.2

USER root

# Crée dossier data partagé si besoin
RUN mkdir -p /opt/data && chown -R airflow:0 /opt/data

# Copie tout le code dans l'image
COPY . /opt

WORKDIR /opt

USER airflow

# Installe Poetry (utile si tu as un pyproject.toml, sinon peux sauter)
RUN pip install poetry

# Installe requirements Python
RUN pip install -r requirements.in
RUN pip install .

WORKDIR /opt/airflow
