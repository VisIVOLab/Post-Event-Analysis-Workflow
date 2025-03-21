# AirflowDemo
Demo sample per l'uso di Airflow con Agisoft Metashape

'''
airflow_project/
├── dags/
│   ├── metashape_dag.py
├── task/
│   ├── import_photos.py
│   ├── align_cameras.py
│   ├── build_point_cloud.py
│   ├── build_mesh.py
│   ├── build_texture.py
│   ├── export_results.py
│   ├── metashape_utils.py  # (Per funzioni comuni)
│   ├── __init__.py
'''

ha senso strutturare il return dell'istanza tra i vari task?
o faccio aprire il progetto? di volta in volta?
