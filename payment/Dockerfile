FROM python:3.12-slim

WORKDIR /home/flask-app

COPY payment/requirements.txt .
RUN pip install -r requirements.txt

COPY common/ ./common/
COPY payment/app.py payment/tpc.py payment/saga.py ./

ENV PYTHONUNBUFFERED=1

CMD ["gunicorn", "-k", "gevent", "-b", "0.0.0.0:5000", "-w", "1", "--timeout", "30", "--log-level=info", "app:app"]
