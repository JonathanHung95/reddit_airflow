FROM puckel/docker-airflow:1.10.9

COPY "requirements.txt" "./"

RUN  pip install --upgrade pip && pip3 install -r requirements.txt

ENV PATH = "$PATH:/usr/local/airflow/.local/bin"