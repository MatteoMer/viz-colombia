.PHONY: ingest download convert clean covariates pipeline dashboard test validate

ingest: download convert

download:
	uv run python scripts/download.py

convert:
	uv run python scripts/convert.py

covariates:
	uv run python scripts/download_covariates.py
	uv run python scripts/build_covariates.py

pipeline: covariates
	uv run python scripts/phase4_refpop.py
	uv run python scripts/phase4_signals.py
	uv run python scripts/phase5_composite.py
	uv run python scripts/phase5_context.py
	uv run python scripts/phase5_bootstrap.py

dashboard: pipeline
	uv run python scripts/build_dashboard.py

test:
	uv run pytest tests/ -v

validate: pipeline
	uv run python scripts/phase6_validation.py

clean:
	rm -rf data/raw/*/batch_* data/parquet
