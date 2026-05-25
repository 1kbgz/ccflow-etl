# Development

Install the package with development dependencies:

```bash
uv pip install -e .[develop]
```

Run the package tests:

```bash
python -m pytest ccflow_etl/tests -q
```

Run Ruff before release:

```bash
python -m ruff check ccflow_etl
python -m ruff format --check ccflow_etl
```

Run the docs checks:

```bash
python -m mdformat --check README.md docs/src/*.md
python -m codespell_lib README.md docs/src/*.md
yardang build
```

Default tests should use synthetic local fixtures and must not require live HTTP, S3, database, Celery, provider credentials, or other external services. Integration tests that need external services should be opt-in and skipped by default.
