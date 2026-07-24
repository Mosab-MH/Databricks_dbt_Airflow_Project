import time
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator 
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState,RunResultState
import pendulum

@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(year=2026,month=7,day=24,tz="UTC")
)
def orchestrate():


    @task
    def ingest_cdc():

        ws = WorkspaceClient(
        host="**************************************",
        token ="*************************************"
        )

        job_trigger = ws.jobs.run_now(job_id="319047234973484")

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)
        
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED,RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("job completed successfully!")
                    break 
                else:
                    raise Exception(f"job failed with state: {job_run.state.result_state}")

            time.sleep(5) 

            return "CDC ingestion completed"

    @task
    def clean_target_directory():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"


    @task.bash
    def source_freshness():
        # Manually set the working directory using 'cd' command before executing the dbt command 
        return "rm -rf /opt/airflow/walmart_project/target && cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id='silver_technical',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_t'
    )

    silver_business = BashOperator(
        task_id='silver_business',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_b'
    )


    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_b'
    )

    gold_ephemeral = BashOperator(
        task_id='gold_ephemeral',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/ephemeral'
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/fact'
    )

    ingest_cdc() >> clean_target_directory() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()