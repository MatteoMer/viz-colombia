.PHONY: ingest download convert clean

ingest: download convert

download:
	uv run python scripts/download.py

convert:
	uv run python scripts/convert.py

clean:
	rm -rf data/raw/*/batch_* data/parquet
