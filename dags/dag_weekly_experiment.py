from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/decathlon_analytics_engineer"

default_args = {
    "owner": "analytics_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["data-team@decathlon.com"],
}

with DAG(
    dag_id="decathlon_domyos_weekly_experiment",
    default_args=default_args,
    description="Pipeline dbt hebdomadaire - Expérimentation retrait kit haltères 10kg",
    schedule_interval="0 6 * * 1",  # Chaque lundi à 06h00 UTC
    start_date=datetime(2023, 8, 28),
    catchup=False,
    tags=["dbt", "domyos", "experiment", "weekly"],
) as dag:

    check_sources_freshness = BashOperator(
        task_id="check_sources_freshness",
        bash_command=f"dbt source freshness --project-dir {DBT_PROJECT_DIR} --target prod",
    )

    run_intermediate = BashOperator(
        task_id="run_intermediate",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --target prod --select int_experiment_sales_enriched",
    )

    run_mart = BashOperator(
        task_id="run_mart",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --target prod --select mart_experiment_weekly_sales",
    )

    run_tests = BashOperator(
        task_id="run_tests",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --target prod --select mart_experiment_weekly_sales",
    )

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_data_team",
        message=":white_check_mark: *mart_experiment_weekly_sales* mise à jour avec succès. Données disponibles pour Tableau.",
        channel="#data-pipelines",
        trigger_rule="all_success",
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id="slack_data_team",
        message=":x: Échec du pipeline `decathlon_domyos_weekly_experiment`. Vérifier les logs Airflow.",
        channel="#data-pipelines",
        trigger_rule="one_failed",
    )

    check_sources_freshness >> run_intermediate >> run_mart >> run_tests >> [notify_success, notify_failure]