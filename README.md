## How to run the application

#### General prerequisites
* Ubuntu 20.04 linux distribution
* The repository is cloned (`git clone https://github.com/dalekseevs/lastfm.git`)
* User is in the root of the repository (`cd lastfm`)

### Run local tests
#### Prerequisites
* Python 3.8 is installed
* pip is installed
* Java Runtime Environment (JRE) 8 is installed
#### Steps
1. Install the required dependencies by running `pip install -r requirements.txt -r test-requirements.txt`
2. Run the test suite by running `pytest`

### Deploy and run the application
#### Prerequisites
* Docker Engine is available and running
#### Steps
1. Run the `./deploy.sh` script
2. Once the application is finished, the output file will be saved in `~/lastfm-analyzer-data/output/top-songs.tsv`

## Assumptions
### Infrastructure
* The application is deployed on a single machine.
* The application is deployed on a machine with at least 8GB of RAM, 8 CPU cores and 20GB of free disk space. (https://spark.apache.org/docs/latest/hardware-provisioning.html)
### Data analysis
* Source files contain a file called `userid-timestamp-artid-artname-traid-traname.tsv` that can be interpreted using the following schema: 
    ```json
    {
      "user_id": "string",
      "timestamp": "timestamp",
      "musicbrainz_trackid": "string",
      "artist_name": "string",
      "musicbrainz-track-id": "string",
      "track_name": "string"
    }
    ```
* Source data is of high quality and does not contain any errors.
* Source data does not exceed the available memory/ disk space.


## Improvements
* Make deploy script more efficient by checking if source files are already downloaded.
* Optimize parallelization and resource allocation for Spark jobs.
* Add more tests, test cases and benchmarks to ensure the correctness and scalability of the solution.
* Add a CI/CD pipeline script to automate the integration and deployment process.
* Better application monitoring.