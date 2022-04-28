FROM python:3.9-slim-buster
RUN apt-get update && \
    apt-get update && apt-get install -y curl apt-transport-https gnupg2 && \
    apt-get install -y curl apt-transport-https gnupg2 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools && \
    apt-get install -y netcat && \
    apt-get install vim -y && \
    pip3 install apache-airflow[celery,redis,postgres,crypto,pandas,mssql]==2.2.3 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.9.txt" && \
    pip install pyspark==3.0.0  && \
    apt-get install g++ unixodbc-dev -y && \
    apt install curl mlocate default-jdk -y && \
	apt install wget -y && \
	wget https://dlcdn.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz && \
	tar xvf spark-3.0.3-bin-hadoop2.7.tgz && \
	mv spark-3.0.3-bin-hadoop2.7/ /opt/spark 
WORKDIR /root/airflow
COPY config/variables.json variables.json
COPY config/connections.yml connections.yml
COPY config/setup_connections.py setup_connections.py
COPY config/requirements.txt requirements.txt
COPY config/airflow.cfg airflow.cfg
COPY entrypoint.sh entrypoint.sh
COPY dags/ dags
ENTRYPOINT ["./entrypoint.sh"]