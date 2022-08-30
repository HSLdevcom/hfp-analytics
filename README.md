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
**Required tools**: docker, docker-compose and Python 3, Postgresql.

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

### Run Migra
```
TODO
```

### Run tests
```
TODO
```

## Deployment

The API is hosted in [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/), and the database in [Azure Database for PostgreSQL](https://azure.microsoft.com/en-us/services/postgresql/)

### Deploy api
```
./deploy_api.sh
```

After this, restart `api` function from Azure portal. After we have a working CI, this step shouldn't be no longer needed.

You can get the API key used to access to the API from Azure Portal -> hfp-analytics rg -> hfp-analytics-api function ->  App keys -> default. See instructions for using the API from `<API url>/docs?code=<API key>`. 

### Deploy importer
```
./deploy_importer.sh
```

After this, restart `importer` function from Azure portal. After we have a working CI, this step shouldn't be no longer needed.

### View logs

From Azure Portal select a function app and open logstream view.

## Contact

The tool is being developed by the HSL InfoDevOps team.