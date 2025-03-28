# AirflowDemo
The repository contains a series of tests and demos using Apache Airflow, an open-source platform used to programmatically author, schedule, and monitor workflows. It is primarily used for managing complex data pipelines or workflows in the data engineering and data science fields.

The project includes automated workflow for photogrammetry and machine learning processing using **Apache Airflow** for managing tasks, **Agisoft Metashape** for image processing, and (...) machine learning in python language.

# Overview

The workflow in this repository is designed to:

1. Acquire image sets from a predefined source.

2. Use Metashape to perform photogrammetry processing (e.g., image alignment, point cloud generation, mesh creation, orthophoto generation, etc.).

3. Monitor task execution and handle any errors using Apache Airflow's scheduling system.
4. ...

# Prerequisites

To successfully run this workflow, you'll need:

- Python 3.8+ (to create a v_env and run scripts)

- Docker

- Apache Airflow installed and configured (preferably in a virtual environment).

- Agisoft Metashape (full version for photogrammetry tasks).


### Installing Metashape

Metashape is a paid software, so you'll need to download and install the pro version from the official Metashape website and the relative  [python module](https://www.agisoft.com/downloads/installer/).

'''code '''

### Installing Apache Airflow

You can install Apache Airflow by following the official instructions on the [Apache Airflow website](https://airflow.apache.org/docs/apache-airflow/stable/installation/).

If you haven't used Airflow before, you can set it up with these commands for a basic configuration:

# Running Locally

To run the project locally, you'll need to:

### Configuring Airflow

## Project Structure (TODO)
The [dags](dags) folder contains the implemented DAGs and is organized as follows:

```
AirflowDemo/
├── dags/
│   └── metashape_dag.py # DAG for the photogrammetry workflow
├── task/
│   ├── import_photos.py
│   ├── align_cameras.py
│   ├── build_point_cloud.py
│   ├── build_mesh.py
│   ├── build_texture.py
│   ├── export_results.py
│   └── metashape_utils.py  # funzioni comuni
```
### Run the Airflow DAG
To run the Airflow DAG, follow these steps:

1. Start Apache Airflow in two CLI:

```bash
airflow scheduler
airflow webserver
```

2. Visit the Airflow web interface (usually at http://localhost:8080), select your DAG, and trigger it manually or schedule it for automatic execution.

## DAGs

### Photogrammetry

<center><img src="img/from_point_cloud.png" width="600" align="center"></center>




ha senso strutturare il return dell'istanza tra i vari task?
o faccio aprire il progetto? di volta in volta?
