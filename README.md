# Distributed Data Systems — Group 20

Aditya Patil · Danil Vorotilov · Pedro Gomes Moreira · Ruben Van Seventer · Veselin Mitev

Three services — order, stock, and payment — coordinate so that checkout never oversells stock or double-charges users. We implement that in two ways: Two-Phase Commit (2PC) for synchronous, strongly consistent commits, and SAGA over Kafka for an event-driven, asynchronous flow. Same API and test harness for both; the active mode is chosen at deploy time.

Sections: [Project structure](#project-structure) · [Deployment](#deployment) · [Testing](#testing) — includes [Testing Failover and Fault-Tolerance](#testing-failover-and-fault-tolerance), [Benchmarking](#benchmarking), [Our Own Locust Testing](#our-own-locust-testing), [Testing Suite](#testing-suite)

## Project structure

Folders and purpose:

- `env/` — environment files used by the compose stack (runtime variables for services and databases)
- `patroni/` — Patroni/Postgres replication configuration and container setup for replication and failover
- `order/` — order microservice
- `payment/` — payment microservice
- `stock/` — stock microservice
	- `post_bootstrap.sh` - Small script that runs inside the DB containers to initialize the databases
	- `db_utils.py` - Common utilities for the database, such as connection management and query execution
- `test/` — test harness and suites (end-to-end; functional, protocol, and stress tests)

## Deployment

***Requirements:*** You need to have docker and docker compose installed on your machine. 

In the root of the project, run `docker compose up`, or `docker compose up --build` if you changed anything and need to rebuild the containers.

You can stop the deployment with `docker compose down` or via Docker Desktop's UI.

When developing you can use the following command to properly (re)deploy everything (stopping and deleting the previously running containers and their data if applicable):
```bash
docker compose down -v --remove-orphans && docker compose up --build -d --wait
```

Wait ~30s-1min for the containers to fully initialize. Check with:
```bash  
docker ps
```
that all of the database containers are healthy. Seeing `health: starting` or `unhealthy` is normal.

## Testing

***Requirements:*** Python >=3.10 

Make a Python environment with the required packages:
```sh
python -m venv venv

# Activate with
source venv/bin/activate 
# Or on Windows:
.\venv\Scripts\Activate.ps1 

pip install -r requirements.txt

# You can deactivate with:
deactivate
```

Deploy the project:
```
docker compose up
```

### Testing Failover and Fault-Tolerance
You can stop any of the database containers at any point (at most one at a time), with `docker kill <container_id>`. The system should continue working, although some errors and failed requests can occur in the first few seconds.

### Benchmarking
Use the [provided benchmarking repo](https://github.com/delftdata/wdm-project-benchmark/tree/master). There are good instructions there, but in short:

```sh
git clone https://github.com/delftdata/wdm-project-benchmark.git
cd wdm-project-benchmark
python -m venv venv
# Activate with
source venv/bin/activate 
# Or on Windows:
.\venv\Scripts\Activate.ps1 
pip install -r requirements.txt
```

Then, check for consistency correctness with:
```sh
cd consistency-test
python run_consistency_test.py
```

Stress test with:
```
cd stress-test
python init_orders.py
locust -f locustfile.py --host="localhost"
```
Go to [http://localhost:8089/](http://localhost:8089/) to use the Locust UI. Set the number of users and the Ramp up, then press Start.

### Our Own Locust Testing

We use locust to do stress testing. To run these tests, follow these instructions:

- Run the project as usual with `docker compose up --build`.
- Navigate to `cd test/stress`.
- Run locust: `locust -f locustfile.py --host="localhost"`.
- Locust will give you a URL where you can run the tests based on the locustfile that it found in the directory.

### Testing Suite

Execute these commands from the root directory. That is it.

```bash
cd test/custom
python run.py
```

When you run the command, this is the flow. You are prompted to choose `TPC` or `SAGA`; the runner patches `docker compose.yml`, restarts the stack, and waits until the Gateway responds. Then it runs the Common Suite, then the Protocol Suite — 2PC for `TPC`, SAGA for `SAGA`. Press `Enter` to run each test case and proceed to the next. At the end you get a Summary and Exit Code.

To use another host or port, set `BASE_URL` before running (e.g. `BASE_URL=http://192.168.1.10:8000 python run.py`).
