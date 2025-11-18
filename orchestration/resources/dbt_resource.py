"""dbt resource configuration for Dagster."""

from pathlib import Path

from dagster_dbt import DbtCliResource


# path to dbt project root
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent

# configured dbt resource
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,  # profiles.yml is in project root
)
