FROM python:3.12-slim

WORKDIR /home/flask-app

COPY order/requirements.txt .
RUN pip install -r requirements.txt

COPY common/ ./common/
COPY order/order_app.py order/db.py order/tpc.py order/saga.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "order_app.py"]
