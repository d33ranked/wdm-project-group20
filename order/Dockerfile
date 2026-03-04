FROM python:3.12-slim

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /home/flask-app

# Copy requirements from the service folder
COPY ./order/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared proto file and generate stubs
COPY ./common/transaction.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. transaction.proto

# Copy the actual service code
COPY ./order/ .

CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]

EXPOSE 5000

ENV PYTHONUNBUFFERED=1
