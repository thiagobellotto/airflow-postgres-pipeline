FROM apache/airflow:3.3.2-python3.12
ENV PYTHONPATH=/opt/airflow
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root etl /opt/airflow/etl
COPY --chown=airflow:root config/demo-passwords.json /opt/airflow/demo-passwords.json
