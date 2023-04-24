## How to run the application

#### General prerequisites
* Ubuntu 20.04 linux distribution

### Run local tests
#### Prerequisites
* Python 3.8 is installed
* pip is installed
* Java Runtime Environment (JRE) 8 is installed
#### Steps
1. Install `requirements.txt` and `test-requirements.txt` (`pip install -r requirements.txt -r test-requirements.txt`)
2. Run `pytest` in the root directory of the project

### Deploy and run the applicaiton
#### Prerequisites
* Docker engine is available and running
#### Steps
1. Clone the repository
2. Run the `deploy.sh` script
3. The output file will be saved in `~/lastfm-analyzer-data/output/top-songs.tsv`

## Assumptions
* The application is deployed on a single machine.
* The application is deployed on a machine with at least 8GB of RAM, 8 CPU cores and 20GB of free disk space. (https://spark.apache.org/docs/latest/hardware-provisioning.html)

## Improvements
* Make deploy script idempotent.
* Add more tests, test cases and benchmarks to ensure the correctness and scalability of the solution.
* Add a CI/CD pipeline script to automate the integration and deployment process.
* Better application monitoring.