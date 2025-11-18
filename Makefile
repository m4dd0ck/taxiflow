.PHONY: build test clean run dagster manifest

build:
	uv sync

run:
	uv run dbt run

test:
	uv run dbt test

clean:
	rm -rf target/ dbt_packages/ logs/

deps:
	uv run dbt deps

docs:
	uv run dbt docs generate
	uv run dbt docs serve

# generate dbt manifest for dagster
manifest:
	uv run dbt parse

# run dagster dev server
dagster:
	uv run dagster dev -m orchestration.definitions

# run full pipeline via dagster
pipeline:
	uv run dagster job execute -m orchestration.definitions -j taxiflow_full_pipeline
