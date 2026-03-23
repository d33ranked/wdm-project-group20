FROM python:3.12-slim

WORKDIR /home/flask-app

COPY stock/requirements.txt .

RUN pip install -r requirements.txt

COPY common/ ./common/
COPY stock/ .

EXPOSE 5000

ENV PYTHONUNBUFFERED=1

CMD ["python", "stock_app.py"]
