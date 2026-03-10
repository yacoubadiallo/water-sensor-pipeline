from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Paramètres par défaut pour le DAG
default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,                  # Nombre de retry en cas d'échec
    "email": ["admin@example.com"], # Remplace par ton email
    "email_on_failure": True,       # Envoie un email si la tâche échoue
    "email_on_retry": False         # Pas d'email sur retry
}

# Définition du DAG
with DAG(
    dag_id="water_sensor_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *", # Toutes les 5 minutes
    catchup=False,
    description="Pipeline dbt pour Water Sensor avec alertes email",
) as dag:

    # Étape 1 : Exécuter dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec dbt dbt run"
    )

    # Étape 2 : Exécuter dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec dbt dbt test"
    )

    # Définir l’ordre d’exécution
    dbt_run >> dbt_test