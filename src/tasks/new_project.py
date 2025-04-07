from airflow.models import TaskInstance

def new_project(**kwargs):
    import Metashape
    import logging

    """
    New Metashape project
    """
    # Check compatibility
    compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version))
    
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

    doc = Metashape.Document()
    doc.save(path=project_path, version="new project")
    logging.info("New project!")