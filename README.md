# Distributed Data Systems — Group 20

Aditya Patil · Danil Vorotilov · Pedro Gomes Moreira · Ruben Van Seventer · Veselin Mitev

Three services — order, stock, and payment — coordinate so that checkout never oversells stock or double-charges users. We implement that in two ways: Two-Phase Commit (2PC) for synchronous, strongly consistent commits, and SAGA over Kafka for an event-driven, asynchronous flow. Same API and test harness for both; the active mode is chosen at deploy time.

Sections: [How To Test](#how-to-test) · [Implementation](#implementation) · [Parameter Tuning](#parameter-tuning) · [Benchmarking Suite](#benchmarking-suite)

## How To Test

Execute these commands from the root directory. That is it.

```bash
cd test/custom
pip install -r requirements.txt
python run.py
```

When you run the command, this is the flow. You are prompted to choose `TPC` or `SAGA`; the runner patches `docker-compose.yml`, restarts the stack, and waits until the Gateway responds. Then it runs the Common Suite, then the Protocol Suite — 2PC for `TPC`, SAGA for `SAGA`. Press `Enter` to run each test case and proceed to the next. At the end you get a Summary and Exit Code.

To use another host or port, set `BASE_URL` before running (e.g. `BASE_URL=http://192.168.1.10:8000 python run.py`).

## Implementation

## Parameter Tuning

## Benchmarking Suite
