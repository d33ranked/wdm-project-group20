FROM python:3.12-slim

WORKDIR /home/flask-app

COPY stock/requirements.txt .

RUN pip install -r requirements.txt

COPY stock/ .
COPY common/db_utils.py .

EXPOSE 5000

ENV PYTHONUNBUFFERED=1
