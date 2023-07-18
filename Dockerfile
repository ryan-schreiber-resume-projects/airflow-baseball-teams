
FROM apache/airflow:2.6.3

USER root

# install Git
RUN sudo apt-get update && sudo apt-get install -y git

# source: https://stackoverflow.com/questions/58292862/how-to-install-homebrew-on-ubuntu-inside-docker-container
RUN apt-get update && \
    apt-get install -y apt wget default-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz 
RUN tar -xvzf spark-3.2.0-bin-hadoop3.2.tgz
RUN mv spark-3.2.0-bin-hadoop3.2 /opt/spark

RUN export SPARK_HOME=/opt/spark
RUN export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

RUN source ~/.bashrc

USER airflow

ADD requirements.txt . 
RUN pip install --trusted-host pypi.org --default-timeout=1000 --trusted-host files.pythonhosted.org apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

