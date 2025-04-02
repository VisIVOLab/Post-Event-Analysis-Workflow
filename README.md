# AirflowDemo
The repository contains a series of tests and demos using **Apache Airflow**, an open-source platform used to programmatically author, schedule, and monitor workflows. It is primarily used for managing complex data pipelines or workflows in the data engineering and data science fields.

The project includes an python automated workflow for photogrammetry and machine learning processing, managing tasks with **Agisoft Metashape** for images and 3D model processing, and machine learning ...


# Overview

The workflow in this repository is designed to:

1. Acquire image sets from a predefined source.

2. Use Metashape to perform photogrammetry processing (e.g., image alignment, point cloud generation, mesh creation, ~~orthophoto generation,~~ etc.).

3. ~~Monitor task execution and handle any errors using Apache Airflow's scheduling system.~~

4. ML ...

# Requirements

To successfully run this workflow, you'll need:

- Python 3.8+ (to create a virtual environment and run scripts)
- Docker
- Apache Airflow 2.10
- Agisoft Metashape 2.2.0 (pro version)
- ...


## Installing Metashape

Metashape: download and install the pro version from the official Metashape website and the relative [python3 module](https://www.agisoft.com/downloads/installer/).

```bash
python3 -m pip install Metashape-2.2.0-cp37.cp38.cp39.cp310.cp311-abi3-linux_x86_64.whl
```

## Installing Apache Airflow

You can install Apache Airflow by following the official instructions on the [Apache Airflow website](https://airflow.apache.org/docs/apache-airflow/stable/installation/) and [Apache Airflow GitHub](https://github.com/apache/airflow).

If you haven't used Airflow before, you can set it up with these commands for a basic configuration:

### Running Locally

`pip` installation is currently officially supported:

```bash
pip install "apache-airflow[celery]==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt"
```

It requires to change the constraints extension to match the Python compatibility version.

To initialize the Airflow database we need to specify the `AIRFLOW_HOME`. By default, it is set to the Airflow home folder. 
```bash
export AIRFLOW_HOME=~/airflow
```
However, you can set it all directly within the same project, allowing you to have different configurations for each individual project.

```bash
export AIRFLOW_HOME="$(pwd)" # from procjet folder
```
To avoid doing it every time the program runs. You can set it on `.bashrc`
```bash
nano ~/.bashrc
# at the end file
export AIRFLOW_HOME="project/path/"
```
Set SQLite database
```bash
airflow db init
```

Set new user on Admin role
```bash
airflow users create --username admin --firstname firstname --lastname lastname --role Admin --email admin@domani.com
```

Run the local server and scheduler. The localhost will open on port 8080.
```bash
airflow scheduler
airflow webserver -p 8080
```

### Running on Docker
Install Docker and Docker-compose. Download the Airflow `docker-compose.yaml`
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
# or
curl -Uri 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml' -o 'docker-compose.yaml'
```
Edit it, changing the `AIRFLOW_CORE_EXECUTOR`, remove `CELERY_RESULT_BACKEND`, `CELERY_BROKER_URL` and `redis` implementation
```yaml 
AIRFLOW_CORE_EXECUTOR: LocalExecutor
```
Create few folders and init docker-compose
```bash 
mkdir ./dags ./logs ./plugins
docker-compose up airflow-init
```
A user account will be set with 'airflow' user and password
```bash 
docker-compose up -d
```
It will set:
- airflow **webserver**
- airflow **scheduler**
- airflow **database**

The localhost will open on port 8080.

## Configuring Local Executor Airflow
Install and migrate to PostgreSQL DB
```bash
sudo apt-get install postgresql postgresql-contrib
```

Open PostgreSQL interface to set a new DB and user
```bash
sudo -u postgres psql

CREATE DATABASE airflow_db;
CREATE USER <user> WITH PASSWORD <password>;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO <user>;
GRANT ALL ON SCHEMA public TO <user>;

\c airflow_db # open db

ALTER SCHEMA public OWNER TO <user>;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO <user>;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO <user>;
GRANT ALL PRIVILEGES ON SCHEMA public TO <user>;

\q # quit
```
Edit `airflow.cfg`
```json
sql_alchemy_conn = postgresql+psycopg2://<user>:<password>@<host>/<db>
executor = LocalExecutor
```

Install `psicopg`
```bash
pip install psycopg2-binary
```
Reset DB and create a new airflow user-account on it
```bash
airflow db reset
airflow db migrate
```

## Project Structure

```
AirflowDemo/
├── dags/
│   ├── dag_photogrammetry_from_depth_map.py            # photogrammetry workflow based on depth map (input params)
│   ├── dag_photogrammetry_from_point_cloud.py          # photogrammetry workflow based on point cloud (input params)
│   ├── dag_photogrammetry_no_save_from_depth_map.py    # photogrammetry workflow based on depth map (no input params)
│   ├── dag_photogrammetry_no_save_from_point_cloud.py  # photogrammetry workflow based on point cloud (no input params)
│   ├── full_workflow_photogrammetry.py                 # single photogrammetry workflow
│   ├── dag_point_classification.py                     # photogrammetry workflow to import masks and export point cloud classification
│   └── ...
├── config/ # Metashape definitions
│   ├── blending_modes.py
│   ├── data_source.py
│   └── ...
├── metashape-module/ # Metashape python module for linux
│   └── Metashape-2.2.0...whl
├── src/ (TODO)
│   ├── import_photos.py
│   ├── align_cameras.py
│   ├── build_point_cloud.py
│   ├── build_mesh.py
│   ├── build_texture.py
│   ├── export_results.py
│   └── metashape_utils.py
└── dagrun.cfg  # define settings and parameters for DAGs input
```
### Run the Airflow DAG
1. Visit the Airflow web interface (usually at http://localhost:8080), select your DAG, and trigger it manually or schedule it for automatic execution

2. For DAGs that require `dagrun.cfg` as an input parameter, execute:
    ```bash
    airflow dags trigger example_dag --conf "$(cat dagrun.cfg)"
    ```

# DAGs structures

The images demonstrate in different color the distinct dependencies and management of tasks for machine learning, photogrammetry, data import, and data export

<center><img src="img/ML_Photo_from_point_cloud.png" width="600" align="center"></center>

<center><img src="img/from_point_cloud.png" width="600" align="center"></center>

<center><img src="img/from_depth_maps.png" width="600" align="center"></center>
