# Setup, configuration and deployment

## Environments

| | Development (default) | Production |
|---|---|---|
| Start | `cp .env.example .env` then `docker compose up -d --build` | `cp .env.prod.example .env.prod`, set the secrets, then `docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d --build` |
| Code | Bind-mounted from the checkout (edits apply without a rebuild) | Baked into the images (what was built and tested is what runs) |
| Postgres port | Published on `localhost:5433` | Not published (only the containers reach it) |
| Spark UI | `localhost:4040` while a job runs | Not published |
| Dashboard | Optional (`--profile dashboard`) | Always on |
| Logs | Text | JSON (`LOG_FORMAT=json`), one object per line with `env`, `run_id` and `task` |
| Restart policy | `unless-stopped` | `always` |

The production overrides are in [`docker-compose.prod.yml`](../docker-compose.prod.yml). Both files are validated by CI.

## Configuration

All settings are environment variables. Docker Compose reads them from `.env` (development) or from the `--env-file` you pass (production). **Credentials have no defaults anywhere:** without them, Compose refuses to start, and the scripts stop with a `ConfigError` that says what to do.

| Variable | Default | Meaning |
|---|---|---|
| `APP_ENV` | `dev` | Environment name, included in JSON logs |
| `PG_USER`, `PG_PASSWORD` | none (required) | Warehouse credentials |
| `PG_DB` | `shop` | Database name |
| `PG_HOST_PORT`, `PREFECT_PORT`, `DASHBOARD_PORT` | 5433, 4200, 8501 | Host ports |
| `SCHEDULE_TZ`, `DAILY_CRON`, `MAINTENANCE_CRON` | `Asia/Kolkata`, `0 2 * * *`, `0 3 * * 0` | Schedules |
| `ALERT_WEBHOOK_URL` | empty | Slack-compatible webhook for failed/crashed/cancelled runs |
| `LOG_FORMAT` | `text` | `text` or `json` |
| `WATERMARK_LOOKBACK_MINUTES` | 10 | Overlap window re-read on each incremental extract |
| `SCHEMA_DRIFT_ALLOW_REMOVED_COLUMNS` | `false` | `true` fills columns removed in the source with NULL instead of stopping |
| `SILVER_FULL_REFRESH` | `false` | `true` makes Silver ignore its checkpoint and re-read all of Bronze |
| `SPARK_DRIVER_MEMORY`, `SPARK_MASTER`, `SPARK_SHUFFLE_PARTITIONS` | `2g`, `local[2]`, 8 | Spark resources (measured, see [performance.md](performance.md)) |
| `JDBC_FETCH_SIZE`, `JDBC_BATCH_SIZE`, `JDBC_WRITE_PARTITIONS` | 10000, 10000, 4 | JDBC tuning (measured) |
| `DELTA_ROWS_PER_FILE` | 1000000 | Target rows per Delta data file |
| `BRONZE_LOG_RETENTION`, `BRONZE_FILE_RETENTION` | `interval 30 days`, `interval 7 days` | Time-travel window of Bronze tables |

### Changing the database password

`PG_USER`/`PG_PASSWORD` only take effect when the Postgres volume is **created**. To change the password of an existing database:

```bash
docker compose exec postgres psql -U <current user> -d shop -c "ALTER USER <user> PASSWORD '<new password>'"
# then put the new password in .env / .env.prod and recreate the containers that use it
docker compose up -d
```

## Production checklist

- [ ] Use a long random `PG_PASSWORD`. Prefer injecting secrets from a secrets manager (Docker/Kubernetes secrets, Vault, a cloud secret store) over an `.env.prod` file on disk.
- [ ] Set `ALERT_WEBHOOK_URL` and send a test failure (for example, stop Postgres during a run) to confirm alerts arrive.
- [ ] Put the Prefect UI (4200) and the dashboard (8501) behind a reverse proxy with authentication. Neither has its own login.
- [ ] Back up the `pgdata` volume (for example `pg_dump` on a schedule) and the `lake/` folder (or move it to object storage with versioning).
- [ ] Decide on VACUUM: the weekly maintenance only *reports* what VACUUM would delete until `vacuum=true` is set.
- [ ] Keep `SCHEDULE_TZ` explicit (UTC is easiest to reason about across teams).

## Continuous integration

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push and pull request:

| Job | What it checks |
|---|---|
| `lint` | `ruff` (style, imports, likely bugs, modern syntax) and that the dev and prod Compose files are valid |
| `unit-tests` | pytest with Java 17 + PySpark. Integration tests skip themselves because there is no database here |
| `sql` | On an empty Postgres: `init.sql`, then every migration **twice** (they must be idempotent), then `dbt parse` and `dbt compile` (every model and test rendered to SQL) |
| `docker-build` | Builds the pipeline and dashboard images (with a build cache) |
| `e2e` | The whole stack on the runner ([`scripts/e2e_test.sh`](../scripts/e2e_test.sh)): build, start, run setup with 2,000 orders, run the daily flow twice, run **all** tests (unit + integration) inside the container, and require 0 failed data quality checks. Runs on `main`, pull requests and on demand, after the fast jobs pass |

## Running the tests locally

```bash
docker compose exec pipeline pytest tests -q                 # everything: unit + integration (needs the stack)
docker compose exec pipeline pytest tests -q -m integration  # integration only
docker compose exec pipeline pytest tests -q -m "not integration"
pip install ruff==0.6.9 && ruff check .                      # lint, as in CI
```

Integration tests use their own rows (unique run ids, temporary Delta lakes, a temporary Prefect server) and delete only what they create.
