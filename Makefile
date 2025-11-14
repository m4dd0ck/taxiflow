.PHONY: build test clean run

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
