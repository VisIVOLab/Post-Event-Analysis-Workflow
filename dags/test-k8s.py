from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="alpine_pod_on_node98",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # solo manuale
    catchup=False,
    tags=["k8s", "test"],
) as dag:

    run_alpine_pod = KubernetesPodOperator(
        task_id="run_alpine",
        name="alpine-test-dag",
        namespace="inaf-test2",
        image="alpine:latest",
        cmds=["sh", "-c"],
        arguments=["echo 'ciao dal DAG su juju-98' && sleep 10"],
        kubernetes_conn_id="kube_inaf",  # deve corrispondere alla tua connessione in Airflow
        node_selector={"kubernetes.io/hostname": "juju-a498f0-hammon-98"},
        get_logs=True,
        is_delete_operator_pod=True,
    )

# airflow connections add kube_inaf \
#   --conn-type kubernetes \
#   --conn-host https://kubernetes.hammon.hpc4ai.unito.it\
#   --conn-extra '{
#     "extra__kubernetes__api_key": "token",
#     "extra__kubernetes__verify_ssl": true
#   }'