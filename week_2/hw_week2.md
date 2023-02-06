# Homework: Week 2

## Question 1. Load January 2020 data

I used "zoom" venv so I ran `conda activate zoom`.

I also started Orion server with `prefect orion start` for double-checking flow run state. 

This is the script I ran for loading the data into the GCS Bucket, using as a sample `etl_web_to_gcs`. I've added creation of the data/color directory in `@write_local()` task, and some other tweaks here and there.

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=2)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """"Fix dtype issues"""
    df.lpep_pickup_datetime= pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime= pd.to_datetime(df.lpep_dropoff_datetime)
    
    print(f'rows: {len(df)}')

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) ->Path:
    """Write DataFrame out locally as parquet file"""
 
    #Added creation of the data/color directory:
    Path(f'data/{color}').mkdir(parents=True, exist_ok=True)

    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to gcs"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path = f'{path}', to_path=path)
    return    

   
@flow()
def etl_web_to_gcs() ->None:
    """Main ETL function"""
    color = "green"
    year = 2020
    month = 1  
    dataset_file = f'{color}_tripdata_{year}-{month:02}'  
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path= write_local(df_clean, color, dataset_file)
    write_gcs(path) 


if __name__ == '__main__':
    etl_web_to_gcs()
```

For executing the script I ran `python hw_etl_web_to_gcs.py` from CLI.

* The answer is: 447,770


## Question 2. Scheduling with Cron


I created a deployment from CLI. So I ran:

`prefect deployment build hw_etl_web_to_gcs.py:etl_web_to_gcs -n "Homework ETL" --cron "0 5 1 * *" -a `

I've added "-a" at the end of the command to automatically send it to Prefect API (merging building and applying).

`prefect deployment build --help`

```shell
│ --apply          -a                                 An optional flag to      │
│                                                     automatically register   │
│                                                     the resulting deployment │
│                                                     with the API.           
```

* The answer is: 0 5 1 * * 

## Question 3. Loading data to BigQuery

I used the file from Question 1 to upload feb2019 and mar2019 yellow taxi data.

Here's the script I used with its main flow and its (2 in this case) subflows:

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials 

@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Just reading the parquet file with Pandas"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write data to Big Query"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.hw_rides",
        project_id="dtc-de-375600",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists= "append",
    )

@flow(log_prints=True) 
def etl_gcs_to_bq(year: int, month: int, color: str):

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    
    print(f'Number of rows: {len(df)}')

@flow()
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__=="__main__":
    etl_parent_flow()
```

Creating the deployment (and applying):

`prefect deployment build hw_etl_gcs_to_bq.py:etl_parent_flow -n gcs_to_bq -a`

Calling the agent:

`prefect agent start -q default`

Running deployment from CLI:

`prefect deployment run etl-parent-flow/gcs_to_bq`

After seeing the 'Number of rows' log print for each subflow and adding them together, I got the result.

I've also double-checked using SQL `COUNT()` function once logged into BigQuery.

```SQL
SELECT COUNT(*) FROM `trips_data_all.hw_rides` 
```

* The answer is: 14,851,920


## Question 4. Github Storage Block

I used Linux venv for this question.

After creating the repository and adding Github block from Prefect Orion UI, I built a Docker image using `docker image build -t dieg0sc/prefect:hw .` and the following files:

##### Dockerfile
```dockerfile
FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows 
RUN mkdir -p /opt/prefect/data/green
```

##### docker-requirements.txt
```text
pandas==1.5.2
prefect-gcp[cloud_storage]==0.2.3
protobuf==4.21.11
pyarrow==10.0.1
```

Then I pushed the image using `docker image push dieg0sc/prefect:hw`. I built the deployment using a python script (see below) and executed it with `python flows/docker_deploy.py`

```python
from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from github_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub

github_block = GitHub.load("zoom-hw")
docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow( 
    flow=etl_web_to_gcs,
    name='etl_github',
    infrastructure=docker_block,
    storage=github_block
)

if __name__ == "__main__":
    docker_dep.apply()
```

Finally, I ran the deployment from CLI:

`prefect deployment run etl-web-to-gcs/etl_github`

* The answer is: 88,605


## Question 5. Email or Slack notifications

I will reuse the code from Question 1 to upload apr2019 green taxi data.

For authenticating with prefect cloud I used `prefect cloud login` and followed up with URL.

After that I registered GCP blocks using `prefect block register -m prefect_gcp`.

Then I added GCP creds and GCS Bucket blocks from PCloud UI.

Building deployment:
`prefect deployment build hw_etl_web_to_gcs.py:etl_web_to_gcs -n etl_q5 -a`

Running deployment:
`prefect deployment run etl-web-to-gcs/etl_q5`

Exited the cloud using `prefect cloud logout`.

I've also tested Slack Automation by creating my own workspace and app cause the URL webhook attached to the question didn't work for me. 
I received the notification successfully after flow run completion.

* The answer is: 514,392

## Question 6. Secrets

Created Secret block from the Prefect Orion UI.

* The answer is 8.
