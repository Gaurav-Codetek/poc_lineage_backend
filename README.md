# Lineage Backend

FastAPI backend for table/column lineage, cache refresh, and metadata inference.

## Refined Structure

```
api/                      # Vercel entrypoint
app/
  api/                    # HTTP routes
  cache/                  # Redis cache helpers
  core/                   # Environment/config loading
  data/                   # Static lineage sample data
  db/                     # Database clients used by runtime APIs
  connectors/             # Connector wrappers used by offline jobs
  extractors/             # Databricks metadata extractors
  ingestion/              # Graph ingestion into Neo4j
  intelligence/           # PK/FK/table-type inference engines
  jobs/                   # Scheduled job orchestration
  lineage/                # Lineage graph helper classes
  retrieval/              # Lineage retrieval logic
  services/               # Runtime service layer used by APIs
scripts/                  # Offline/batch entrypoint scripts
```

## API Entrypoints

- `GET /lineage/table/{table}`
- `GET /lineage/table/downstream/{table}`
- `GET /lineage/column/{table}`
- `GET /lineage/column/{table}/{column}`
- `POST /refresh/cache`
- `POST /refresh/ingestion`
- `GET /retriever/lineage/{catalog}.{schema}.{table}`
- `GET /retriever/tables`
- `GET /retriever/tables/catalog`
- `GET /retriever/tables/search/{query}`
- `GET /catalog-metadata/hierarchy`
- `GET /retriever/dimensions/{table_name}`
- `GET /retriever/dimensions/detail/{table_name}`

## Batch Scripts

- `scripts/run_ingestion.py`
- `scripts/run_catalog_metadata_ingestion.py`
- `scripts/run_pk_detection.py`
- `scripts/run_fk_detection.py`
- `scripts/run_fk_scoring.py`
- `scripts/run_table_classification.py`
- `scripts/scheduler.py`
