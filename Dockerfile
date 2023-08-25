

FROM ryanschreiber86/pyspark:latest
FROM apache/airflow:2.6.3

USER airflow

ADD requirements.txt . 
RUN pip install --trusted-host pypi.org --default-timeout=1000 --trusted-host files.pythonhosted.org apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

