"""dbt resource configuration for Dagster."""

from pathlib import Path

from dagster_dbt import DbtCliResource


DBT_PROJECT_DIR = Path(__file__).parent.parent.parent

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,  # profiles.yml is in project root
)
