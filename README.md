# Distributed Data Systems Project Group 20

## Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

## Deployment

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

In the root of the project, run `docker-compose up`, or `docker-compose up --build` if you changed anything and need to rebuild the containers.

You can stop the deployment with `docker-compose down` or via Docker Desktop's UI.

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
docker-compose up
```

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

- Run the project as usual with `docker-compose up --build`.
- Navigate to `cd test/stress`.
- Run locust: `locust -f locustfile.py --host="localhost"`.
- Locust will give you a URL where you can run the tests based on the locustfile that it found in the directory.
