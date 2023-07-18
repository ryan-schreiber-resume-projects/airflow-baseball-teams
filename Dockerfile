FROM apache/airflow:2.6.3

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y ca-certificates && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

ADD requirements.txt . 
RUN pip install --trusted-host pypi.org --default-timeout=1000 --trusted-host files.pythonhosted.org apache-airflow==${AIRFLOW_VERSION} -r requirements.txt