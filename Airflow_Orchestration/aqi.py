from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import traceback
import requests

BASE_PATH    = "/Users/shubham_2022bite076@nitsri.ac.in/capstone"
CLUSTER_ID   = "0411-093803-bgbxgx3v"
CONN_ID      = "capstron"
CATALOG      = "databricks_7405612194732360"
WARNING_BASE_VOLUME = f"Volumes/{CATALOG}/raw_schema/raw/schema_warnings"
KEYS = ["city_day", "city_hour", "station_day", "station_hour"]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_databricks_host_token():
    hook = DatabricksHook(databricks_conn_id=CONN_ID)
    conn = hook.get_connection(CONN_ID)
    host = conn.host.rstrip("/")
    token = conn.password
    return host, token


def _read_volume_file(host, token, relative_path):
    url = f"https://{host}/api/2.0/fs/files/{relative_path}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.text


def _delete_volume_file(host, token, relative_path):
    url = f"https://{host}/api/2.0/fs/files/{relative_path}"
    resp = requests.delete(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 404:
        return
    resp.raise_for_status()


# ─── Callbacks ────────────────────────────────────────────────────────────────

def notify_failure(context):
    ti  = context["task_instance"]
    exc = context.get("exception")

    run_page_url = ti.xcom_pull(task_ids=ti.task_id, key="run_page_url") or "N/A"

    exc_msg = str(exc) if exc else "No exception message captured."
    trace = (
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        if exc else "No traceback."
    )

    send_email(
        to="suvmkr346@gmail.com",
        subject=f"[capstone_aq_pipeline] ❌ {ti.task_id} FAILED — {context.get('logical_date')}",
        html_content=f"""
            <h3>Task Failed: <code>{ti.task_id}</code></h3>
            <p><b>Logical Date:</b> {context.get('logical_date')}</p>
            <p><b>Databricks Run Page:</b> <a href="{run_page_url}">{run_page_url}</a></p>
            <hr>
            <h4>Error Message:</h4>
            <pre>{exc_msg}</pre>
            <h4>Full Traceback:</h4>
            <pre>{trace}</pre>
        """,
    )


# ─── Schema Warning Check ──────────────────────────────────────────────────────

def check_schema_warnings(**context):
    host, token = _get_databricks_host_token()

    dag_run = context["dag_run"]
    bronze_ti = dag_run.get_task_instance("bronze_layer")
    bronze_failed = bronze_ti is not None and bronze_ti.state == "failed"

    warnings_found = []
    for key in KEYS:
        relative_path = f"{WARNING_BASE_VOLUME}/{key}.txt"
        try:
            content = _read_volume_file(host, token, relative_path)
            if content:
                warnings_found.append(f"<b>{key}</b>: {content.strip()}")
                _delete_volume_file(host, token, relative_path)
        except Exception as e:
            print(f"   ⚠️  Could not read/delete warning file for {key}: {e}")

    if bronze_failed:
        run_page_url = context["task_instance"].xcom_pull(task_ids="bronze_layer", key="run_page_url") or "N/A"
        warning_html = (
            "<ul>" + "".join(f"<li>{w}</li>" for w in warnings_found) + "</ul>"
            if warnings_found
            else "<p>No warning files found (notebook raised before writing them).</p>"
        )
        send_email(
            to="suvmkr346@gmail.com",
            subject="[capstone_aq_pipeline] ❌ SCHEMA MISMATCH — bronze_layer failed",
            html_content=f"""
                <h3>Bronze Layer Failed due to Schema Mismatch</h3>
                <p>The bronze notebook raised a <code>ValueError</code> because incoming data
                has columns that are both extra AND different from the expected schema.</p>
                <p><b>Databricks Run:</b> <a href="{run_page_url}">{run_page_url}</a></p>
                {warning_html}
                <p>Silver and Gold layers have been blocked. Fix the source data and re-run.</p>
            """,
        )
        raise AirflowException(
            "bronze_layer failed due to schema mismatch — silver/gold blocked. "
            "Schema mismatch email sent."
        )

    if warnings_found:
        send_email(
            to="suvmkr346@gmail.com",
            subject="[capstone_aq_pipeline] ⚠️ Schema warning — extra columns detected (ignored)",
            html_content=(
                "<p>Bronze completed successfully, but unexpected extra columns were found "
                "and silently dropped:</p>"
                "<ul>" + "".join(f"<li>{w}</li>" for w in warnings_found) + "</ul>"
            ),
        )
        print("   ⚠️  Schema warnings emailed. Continuing to silver/gold.")
    else:
        print("   ✅ No schema warnings found. Bronze clean.")


# ─── Pipeline-level failure notifier ──────────────────────────────────────────

def notify_pipeline_failure(**context):
    dag_run = context["dag_run"]
    ti_self = context["task_instance"]  # has active DB session — use this for xcom_pull

    bad_states = {"failed", "upstream_failed"}
    bad_tasks = [
        ti for ti in dag_run.get_task_instances()
        if ti.state in bad_states and ti.task_id != "notify_pipeline_failure"
    ]

    if not bad_tasks:
        print("   ✅ notify_pipeline_failure triggered but no failed tasks found.")
        return

    rows = ""
    for ti in bad_tasks:
        try:
            run_page_url = ti_self.xcom_pull(task_ids=ti.task_id, key="run_page_url") or "N/A"
        except Exception:
            run_page_url = "N/A"

        state_label = (ti.state or "unknown").upper()
        rows += f"""
            <tr>
                <td><code>{ti.task_id}</code></td>
                <td><b>{state_label}</b></td>
                <td><a href="{run_page_url}">{run_page_url}</a></td>
            </tr>
        """

    send_email(
        to="suvmkr346@gmail.com",
        subject=f"[capstone_aq_pipeline] ❌ Pipeline failure summary — {context.get('logical_date')}",
        html_content=f"""
            <h3>Pipeline Run Failed</h3>
            <p>One or more tasks failed or were blocked:</p>
            <table border="1" cellpadding="6">
                <tr><th>Task</th><th>State</th><th>Databricks Run URL</th></tr>
                {rows}
            </table>
            <p>Check Airflow logs and Databricks run pages for full details.</p>
        """,
    )
    print("   ✅ Pipeline failure summary email sent.")


# ─── Default Args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="capstone_aq_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["capstone"],
) as dag:

    ingestion = DatabricksSubmitRunOperator(
        task_id="databricks_ingestion",
        databricks_conn_id=CONN_ID,
        existing_cluster_id=CLUSTER_ID,
        notebook_task={"notebook_path": f"{BASE_PATH}/Databricks_ingestion"},
        on_failure_callback=notify_failure,
        do_xcom_push=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    bronze = DatabricksSubmitRunOperator(
        task_id="bronze_layer",
        databricks_conn_id=CONN_ID,
        existing_cluster_id=CLUSTER_ID,
        notebook_task={"notebook_path": f"{BASE_PATH}/bronze_layer"},
        on_failure_callback=notify_failure,
        do_xcom_push=True,
        retries=0,
    )

    silver = DatabricksSubmitRunOperator(
        task_id="silver_layer",
        databricks_conn_id=CONN_ID,
        existing_cluster_id=CLUSTER_ID,
        notebook_task={"notebook_path": f"{BASE_PATH}/silver_layer"},
        on_failure_callback=notify_failure,
        do_xcom_push=True,
        retries=0,
    )

    gold = DatabricksSubmitRunOperator(
        task_id="gold_layer",
        databricks_conn_id=CONN_ID,
        existing_cluster_id=CLUSTER_ID,
        notebook_task={"notebook_path": f"{BASE_PATH}/Gold_layer"},
        on_failure_callback=notify_failure,
        do_xcom_push=True,
        retries=0,
    )

    schema_warnings = PythonOperator(
        task_id="check_schema_warnings",
        python_callable=check_schema_warnings,
        trigger_rule=TriggerRule.ALL_DONE,
        on_failure_callback=notify_failure,
        provide_context=True,
    )

    pipeline_failure_alert = PythonOperator(
        task_id="notify_pipeline_failure",
        python_callable=notify_pipeline_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    ingestion >> bronze >> schema_warnings >> silver >> gold
    [ingestion, bronze, schema_warnings, silver, gold] >> pipeline_failure_alert