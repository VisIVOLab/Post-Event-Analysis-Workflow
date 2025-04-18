# Overview

# Requirements


## Project Structure

```
docker/
├── inputs_photogrammetry/
│   └── params.py
├── step_photogrammetry/
│   ├── init_config.py
│   ├── new_project.py
│   ├── import_photos.py
│   ├── match_align_photos.py
│   ├── build_depth_maps.py
└── init_set.json # verrà creato

```

### Run the Docker DAG
1. Settare i parametri di /file e /file

2. Run DAG 
    ```bash
    airflow dags trigger dag_photogrammetry_depth_map_docker --conf "$(cat docker-dagrun.cfg)"
    ```