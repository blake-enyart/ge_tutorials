import airflow
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
import os
import pandas as pd
from sqlalchemy import create_engine
import great_expectations as ge


# Global variables that are set using environment varaiables
GE_TUTORIAL_DB_URL = os.getenv("GE_TUTORIAL_DB_URL")
GE_TUTORIAL_ROOT_PATH = os.getenv("GE_TUTORIAL_ROOT_PATH")
PROD_SCHEMA = os.getenv("PROD_SCHEMA")

great_expectations_context_path = os.getenv(
    "GE_TUTORIAL_GE_CONTEXT_PATH"
) or os.path.join(GE_TUTORIAL_ROOT_PATH, "great_expectations")


default_args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(1)}


# The DAG definition
dag = DAG(
    dag_id="ge_tutorials_dag_with_ge",
    default_args=default_args,
    schedule_interval=None,
)

def load_files_into_db(ds, **kwargs):
    """
    A method to simply load CSV files into a database using SQLAlchemy.
    """

    engine = create_engine(GE_TUTORIAL_DB_URL)

    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {PROD_SCHEMA} ")
        conn.execute(f"drop table if exists {PROD_SCHEMA}.npi_small cascade ")
        conn.execute(f"drop table if exists {PROD_SCHEMA}.state_abbreviations cascade ")

        files = ["state_abbreviations", "contacts", "npi_small"]
        for file_name in files:
            df = pd.read_csv(
                os.path.join(GE_TUTORIAL_ROOT_PATH, "data", f"{file_name}.csv")
            )
            if file_name == "npi_small":
                # Rename headers of dataset
                column_rename_dict = {
                    old_column_name: old_column_name.lower()
                    for old_column_name in df.columns
                }
                df.rename(columns=column_rename_dict, inplace=True)

            # Inject file into SQL table
            df.to_sql(
                file_name,
                engine,
                schema=PROD_SCHEMA,
                if_exists="replace",
                index=False,
                index_label=None,
                chunksize=None,
                dtype=None,
            )

    return "Loaded files into the database"

def publish_to_prod():
    """
    A method to simply "promote' a table in a database by renaming it using SQLAlchemy.
    """
    engine = create_engine(GE_TUTORIAL_DB_URL)

    with engine.connect() as conn:
        conn.execute("drop table if exists prod_count_providers_by_state")
        conn.execute(
            "alter table count_providers_by_state rename to prod_count_providers_by_state"
        )


task_validate_source_data = GreatExpectationsOperator(
    task_id="task_validate_source_data",
    expectation_suite_name="npi_small_file.critical",
    batch_kwargs={
        "path": os.path.join(GE_TUTORIAL_ROOT_PATH, "data", "npi_small.csv"),
        "datasource": "input_files",
    },
    dag=dag,
)

task_load_files_into_db = PythonOperator(
    task_id="task_load_files_into_db",
    provide_context=True,
    python_callable=load_files_into_db,
    dag=dag,
)

task_validate_source_data_load = GreatExpectationsOperator(
    task_id="task_validate_source_data_load",
    assets_to_validate=[
        {
            "batch_kwargs": {
                "path": os.path.join(GE_TUTORIAL_ROOT_PATH, "data", "npi_small.csv"),
                "datasource": "input_files",
            },
            "expectation_suite_name": "npi_small_file.critical",
        },
        {
            "batch_kwargs": {
                "table": "npi_small",
                "schema": PROD_SCHEMA,
                "datasource": "datawarehouse",
            },
            "expectation_suite_name": "npi_small_db_table.critical",
        },
    ],
    dag=dag,
)

task_transform_data_in_db = BashOperator(
    task_id="task_transform_data_in_db",
    bash_command="dbt run --project-dir {} --models staging.*".format(
        os.path.join(GE_TUTORIAL_ROOT_PATH, "dbt")
    ),
    dag=dag,
)

task_transform_data_to_mart = BashOperator(
    task_id="task_transform_data_to_mart",
    bash_command="dbt run --project-dir {} --models marts.*".format(
        os.path.join(GE_TUTORIAL_ROOT_PATH, "dbt")
    ),
    dag=dag,
)

task_validate_analytical_output = GreatExpectationsOperator(
    task_id="task_validate_analytical_output",
    expectation_suite_name="count_providers_by_state.critical",
    batch_kwargs={
        "table": "count_providers_by_state",
        "schema": "great_expectations",
        "datasource": "datawarehouse",
    },
    dag=dag,
)

task_publish = PythonOperator(
    task_id='task_publish',
    python_callable=publish_to_prod,
    dag=dag)


# DAG dependencies
task_validate_source_data >> task_load_files_into_db >> task_validate_source_data_load >> task_transform_data_in_db >> task_transform_data_to_mart >> task_validate_analytical_output >> task_publish
