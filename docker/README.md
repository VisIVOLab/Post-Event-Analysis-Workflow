# Docker Photogrammetry Pipeline

## Overview
To orchestrate the photogrammetry pipeline, this part of the project utilizes the DockerOperator of Apache Airflow to run a required Docker containers for each workflow task. The goal is to give the possibility to run the container on different machines depending on the task's resource needs, such as CPU or GPU requirements (particularly to accommodate machine learning tasks). This allows for optimal resource allocation, like those requiring heavy GPU usage, are executed on machines with the appropriate hardware.

## Requirements
- airflow apache
- docker-compose
- docker container with the required Metashape dependencies pre-installed

## Project Structure
```
dags/
│   └── dag_photogrammetry_docker.py    # docker photogrammetry workflow based on depth map
docker/
├── config/                             # Metashape definitions
├── inputs_photogrammetry/
│   └── params.py                       # input photogrammetric process params
├── step_photogrammetry/
│   ├── init_config.py
│   ├── new_project.py
│   ├── import_photos.py
│   ├── match_align_photos.py
│   ├── build_depth_maps.py
│   ├── build_point_cloud.py
│   ├── build_model.py
│   └── build_tiled.py 
└── init_set.json                       # support file generated during the process
docker-dagrun.cfg                       # input photogrammetric path project
```

### Configuration
`init_set.json` defines the path setting for the volume docker and the default setting for CPU/GPU usage. It is automatically created at the start of the DAG execution.
```json
{
    "project_path": "/photogrammetry/project/project.psx",
    "project_output": "/photogrammetry/project",
    "image_path": "/photogrammetry/images",
    "cpu_enable": false,
    "gpu_mask": 1
}
```
[`docker-dagrun.cfg`](../docker-dagrun.cfg) defines the internal container paths for your input UAV images and output data
```json
{
  "settings": {
    "project_path": "/path/to/output",
    "image_path": "/path/to/images/input"
  }
}
```
[`params.json`](inputs_photogrammetry/params.json) defines the photogrammetry configuration setting
```json
"settings": {
  ...
}
"matchPhotos": {
  ...
},
"alignCameras": {
  ...
},
"buildDepthMaps": {
  ...
},
"buildPointCloud": {
  ...
},
"buildModel": {
  ...
},
"buildTiledModel": {
  ...
}
```

## Run the Docker DAG
1. Set di [`params.json`](inputs_photogrammetry/params.json) and [`docker-dagrun.cfg`](../docker-dagrun.cfg)

2. Run DAG 
    ```bash
    airflow dags trigger dag_photogrammetry_depth_map_docker --conf "$(cat docker-dagrun.cfg)"
    ```