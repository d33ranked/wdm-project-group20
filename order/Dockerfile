FROM python:3.12-slim

WORKDIR /home/flask-app

COPY order/requirements.txt .

RUN pip install -r requirements.txt

COPY order/ .
COPY common/db_utils.py .

EXPOSE 5000

ENV PYTHONUNBUFFERED=1
