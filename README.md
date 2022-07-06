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

> **TODO:** OS and tool requirements, setting up a dev environment, running tests, getting and using test data, inspecting local dev and remote Azure databases as well as generating migration scripts with Migra, etc.

## Deployment

The API is hosted in [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/), and the database in [Azure Database for PostgreSQL](https://azure.microsoft.com/en-us/services/postgresql/)

> **TODO:** Deployment in Azure at a general level (and how to get detailed instructions not published here).

## Contact

The tool is being developed by the HSL InfoDevOps team.