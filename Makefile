.PHONY: build test clean run dagster manifest

build: ## Install the Python dependencies
	uv sync

run: ## dbt run
	uv run dbt run

test: ## dbt test
	uv run dbt test

clean: ## Remove dbt artifacts
	rm -rf target/ dbt_packages/ logs/

deps: ## Install the dbt packages
	uv run dbt deps

docs: ## Generate and serve the dbt docs
	uv run dbt docs generate
	uv run dbt docs serve

manifest: ## Parse the project so Dagster can load it
	uv run dbt parse

dagster: ## Start the Dagster dev server
	uv run dagster dev -m orchestration.definitions

pipeline: ## Run the full Dagster job
	uv run dagster job execute -m orchestration.definitions -j taxiflow_full_pipeline
