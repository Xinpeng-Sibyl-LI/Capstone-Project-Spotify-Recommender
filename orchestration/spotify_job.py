from dagster import op, job, schedule, Nothing, In
import subprocess
import sys
import os
from pathlib import Path

@op
def crawl_spotify_data():
    """Crawl Spotify data using the ingestion script"""
    # Get the project root directory (parent of dagster_project)
    project_root = Path(__file__).parent.parent
    script_path = project_root / "ingestion" / "crawl.py"
    
    # Use the same Python interpreter that's running Dagster
    python_path = sys.executable
    subprocess.run([python_path, str(script_path)], check=True, cwd=str(project_root))

@op(ins={"start": In(Nothing)})
def load_to_snowflake():
    """Load crawled data to Snowflake"""
    project_root = Path(__file__).parent.parent
    script_path = project_root / "ingestion" / "load_to_snowflake.py"
    
    # Use the same Python interpreter that's running Dagster
    python_path = sys.executable
    subprocess.run([python_path, str(script_path)], check=True, cwd=str(project_root))

@op(ins={"start": In(Nothing)})
def run_dbt():
    """Run dbt transformations"""
    project_root = Path(__file__).parent.parent
    dbt_path = project_root / "dbt"
    subprocess.run(["dbt", "run", "--project-dir", str(dbt_path)], check=True, cwd=str(project_root))

@op(ins={"start": In(Nothing)})
def test_dbt():
    """Run dbt tests"""
    project_root = Path(__file__).parent.parent
    dbt_path = project_root / "dbt"
    subprocess.run(["dbt", "test", "--project-dir", str(dbt_path)], check=True, cwd=str(project_root))

@job
def spotify_pipeline():
    """Spotify data pipeline job"""
    crawl = crawl_spotify_data()
    load = load_to_snowflake(start=crawl)
    run = run_dbt(start=load)
    test_dbt(start=run)

@schedule(cron_schedule="0 8 * * *", job=spotify_pipeline, execution_timezone="UTC")
def daily_8am_schedule(_context):
    """Schedule to run pipeline daily at 8 AM UTC"""
    return {}

