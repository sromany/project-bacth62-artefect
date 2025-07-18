FROM apache/airflow:3.0.2
# RUN pip install --index-url https://${{vars.REGISTRY_PYTHON_ZONE}}-python.pkg.dev/${{vars.REGISTRY_PYTHON_PROJECT_ID}}/${{vars.REGISTRY_NAME}}/ project62

WORKDIR /opt
USER root
RUN mkdir config package
COPY ./config/* config
COPY ./dist/* package

COPY requirements.in config/
USER airflow
RUN pip install -r config/requirements.in
RUN pip install package/project_bacth62_artefect-0.1.2.tar.gz
WORKDIR /opt/airflow