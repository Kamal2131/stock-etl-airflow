"""
Master ETL Orchestrator DAG
Coordinates both F&O and Nifty 500 Equity ETL pipelines.

This DAG triggers both child DAGs and can run them in parallel or sequence.
Schedule: Daily at 4:00 PM IST on trading days
"""
import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Configuration
RUN_FNO = os.getenv("RUN_FNO_PIPELINE", "true").lower() == "true"
RUN_EQUITY = os.getenv("RUN_EQUITY_PIPELINE", "true").lower() == "true"
RUN_PARALLEL = os.getenv("RUN_PIPELINES_PARALLEL", "true").lower() == "true"


@dag(
    dag_id="market_etl_orchestrator",
    default_args=default_args,
    description="Master orchestrator for F&O and Equity ETL pipelines",
    schedule="0 16 * * 1-5",  # 4:00 PM IST, Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Don't catch up - child DAGs handle their own backfill
    max_active_runs=1,
    tags=["orchestrator", "master", "etl", "production"],
)
def market_etl_orchestrator():
    """
    Master ETL Orchestrator
    
    Triggers both F&O and Nifty 500 Equity pipelines.
    
    Configuration (via environment variables):
    - RUN_FNO_PIPELINE: Enable/disable F&O pipeline (default: true)
    - RUN_EQUITY_PIPELINE: Enable/disable Equity pipeline (default: true)  
    - RUN_PIPELINES_PARALLEL: Run pipelines in parallel (default: true)
    """
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Trigger F&O ETL DAG
    if RUN_FNO:
        trigger_fno = TriggerDagRunOperator(
            task_id="trigger_fno_etl",
            trigger_dag_id="fno_etl_daily",
            reset_dag_run=True,
            wait_for_completion=not RUN_PARALLEL,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
        )
    else:
        trigger_fno = EmptyOperator(task_id="skip_fno_etl")
    
    # Trigger Nifty 500 Equity ETL DAG
    if RUN_EQUITY:
        trigger_equity = TriggerDagRunOperator(
            task_id="trigger_nifty500_etl",
            trigger_dag_id="nifty500_etl_daily",
            reset_dag_run=True,
            wait_for_completion=not RUN_PARALLEL,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
        )
    else:
        trigger_equity = EmptyOperator(task_id="skip_nifty500_etl")
    
    # Define task dependencies based on parallel/sequential mode
    if RUN_PARALLEL:
        # Parallel execution
        start >> [trigger_fno, trigger_equity] >> end
    else:
        # Sequential execution (F&O first, then Equity)
        start >> trigger_fno >> trigger_equity >> end


# Instantiate the DAG
market_etl_daily = market_etl_orchestrator()
