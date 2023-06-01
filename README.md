# HFP-Analytics

A REST API that

- provides [HFP data](https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/) from the past in a compact format for further use;
- analyzes the expected vs. actual quality of HFP data with various metrics.

The API is under early development and currently not public.

## Features

Presented here roughly in priority order.
See further descriptions from the links.

| Feature | Status |
| ------- | ------ |
| [Normalized HFP data](./docs/data-model-and-io.md) | 🟡 Implemented in DB, not available from the API |
| [Stop correspondence analysis](./docs/analysis-features.md#stop-correspondence-analysis) | 🟢 Implemented |
| [Vehicle analysis](./docs/analysis-features.md#vehicle-analysis) | 🔴 Planned |
| [Journey availability analysis](./docs/analysis-features.md#journey-availability-analysis) | 🔴 Planned |
| [Raw data availability](./docs/analysis-features.md#raw-data-availability) | 🔴 Planned |
| [Geographical area analysis](./docs/analysis-features.md#geographical-area-analysis) | 🔴 Planned |
| [Journey route validity analysis](./docs/analysis-features.md#journey-route-validity-analysis) | 🔴 Planned |

## Development

### Requirements

**Preferred OS**: Linux, haven't tried with Windows or Mac yet.\
**Required tools**: docker, docker-compose and Python 3.10+, Pip3, Postgresql.

### Set up development environment
```
./setup.sh
./run-local.sh
```

`setup.sh` creates the required files for local development.\
`run-local.sh` builds api, importer and DB service docker images and runs them.

After running those scripts, you need to fill in some secret values into `.env` file:
```
HFP_STORAGE_CONTAINER_NAME=secret
HFP_STORAGE_CONNECTION_STRING=secret
```
Values to these can be found from Azure Portal -> hfp-analytics-dev rg -> hfp-analytics-importer -> configuration

Install Python libraries globally on your machine with
```
cd python
pip3 install -r requirements.txt
```
Make tables as timescale DB tables with:
```
SELECT create_hypertable('hfp.hfp_point', 'point_timestamp', chunk_time_interval => INTERVAL '1 day');
```
More information can be found from docs/timescaledb.md

### See API docs

You can get an API key from Azure's key vault: Azure Portal -> hfp-analytics-[environment] -> hfp-analytics-[environment]-vault -> Secrets -> host--masterKey--master. See API docs from `<API url>/docs?code=<API key>`. That app url can be found from `api` Function App's overview page.

### Get test data

To import data from Digitransit, go to `http://localhost:7071/run_import`
Check that test data exists `http://localhost:7071/jore_stops`

Import HFP data (from yesterday) with running
```
./trigger-importer-local.sh
```
Check logs from `importer` to see when the import finishes.

### Inspect local database
```
PGPASSWORD=postgres PGOPTIONS=--search_path=public,api,hfp,stopcorr psql -h localhost -p 5432 -d analytics -U postgres
```

### Inspect remote database
```
PGPASSWORD=<db_password> PGOPTIONS=--search_path=public,api,hfp,stopcorr psql -h <db_host> -p 5432 -d analytics -U <db_username>
```

Get the required secrets from Azure portal.


### Run tests
```
TODO
```

## Deployment

The API is hosted in [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/), and the database in [Azure Database for PostgreSQL](https://azure.microsoft.com/en-us/services/postgresql/)

### Manually deploy Api & importer

You can deploy e.g. unmerged PR to run in dev manually for testing. Currently manual deploying is supported for dev environment only.
```
cd scripts/manual_deploy/
./deploy_api.sh
./deploy_importer.sh
```
After this, restart `api` and `importer` functions from Azure portal as Azure's continuous deployment is slow to pull new versions for image's at least for now.

### Deploy Api & importer via Azure pipeline

Open hfp-analytic's Azure Pipelines page, click 3 dots from some environments a pipeline, select run pipeline. After pipeline has been run, you can manually restart Azure Function Apps to ensure that they start running with the latest image.

### Deploy schema with Migra

To make for example dev db schema the same as local db schema, cd into `scripts/migra_dev` and run:

```
python3 migra_local_vs_dev init
```
Open the generated .sql file and inspect the changes to be applied. If everything is OK, you can apply changes with:
```
python3 migra_local_vs_dev apply
```
Note: you may want to comment out SQL related to timescaledb / postgis updates.

### Manually trigger importer or analyzer

Fucntion can be started with python script `scripts/trigger_function.py `

You can specify the environment and / or the function to be triggered. The function is required, the env is local by default.

Examples:

Trigger analyzer locally
```
python3 scripts/trigger_function.py analyzer
```

Trigger importer on dev
```
python3 scripts/trigger_function.py --env dev importer
```

Show help
```
python3 scripts/trigger_function.py -h
```

In order to trigger functions on Azure, you'll have to get a master API key from Azure Portal -> hfp-importer -> App keys -> _master


### View logs

Two ways to inspect logs:
1) SSH into local / dev / test / prod db, query logs from either `api_log` or `importer_log` tables.
2) From Azure Portal select a function app and open logstream view.

## Contact

The tool is being developed by the HSL InfoDevOps team.