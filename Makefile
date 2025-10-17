init: ## initial setup
		uv tool install pre-commit
		uv tool install ruff
		uv tool install black
		uv tool install pytest
		uv sync
run:
		python src/fat_cats_with_polars/run.py x.csv y.csv
