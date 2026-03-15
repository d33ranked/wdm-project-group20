FROM python:3.12-slim

WORKDIR /home/flask-app

COPY payment/requirements.txt .
RUN pip install -r requirements.txt

COPY common/ ./common/
COPY payment/payment_app.py payment/kafka_handler.py payment/recovery.py ./

ENV PYTHONUNBUFFERED=1

CMD [ "python", "payment_app.py"]
