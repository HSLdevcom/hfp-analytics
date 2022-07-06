# HFP-Analytics

A REST API that 

- provides [HFP data](https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/) from the past in a compact format for further use;
- analyzes the expected vs. actual quality of HFP data with various metrics.

The API is under early development and currently not public.

## Features

Presented here roughly in priority order.
See further descriptions from the links.

| Feature | Status |
| - | - |
| [Normalized HFP data](./docs/data-model-and-io.md) | 🟡 Implemented in DB, not available from the API |
| [Stop correspondence analysis](./docs/analysis-features.md#stop-correspondence-analysis) | 🟢 Implemented |
| [Vehicle analysis](./docs/analysis-features.md#vehicle-analysis) | 🔴 Planned |
| [Journey availability analysis](./docs/analysis-features.md#journey-availability-analysis) | 🔴 Planned |
| [Raw data availability](./docs/analysis-features.md#raw-data-availability) | 🔴 Planned |
| [Geographical area analysis](./docs/analysis-features.md#geographical-area-analysis) | 🔴 Planned |
| [Journey route validity analysis](./docs/analysis-features.md#journey-route-validity-analysis) | 🔴 Planned |

## Usage & development

### Requirements

The tool is created using PostgreSQL 13 & PostGIS 3.0, Python 3.7, QGIS 3.16 and docker-compose 1.29.0.
Docker and docker-compose should be enough to run the whole process, but you for the time being you will need local QGIS installation to produce the map images.

### Testing

Run `./test-run.sh`: this should start the docker-compose services using `docker-compose.test.yml` and example data in `testdata/import`.
In the prompt you should see the db startup, data import, analysis and reporting steps the same way they are meant to work in production.
However the test run does not produce or use map images since they have to be created manually from QGIS Atlas.

### Deployment

Clone this project, and navigate to the project root.
Run the setup script that will create required directories, `.env` and `docker-compose.yml` files, build the Python Docker image with required packages, and create a common Docker network for the compose services:

```
./setup.sh
```

Modify `.env` and `docker-compose.yml` according to your local environment.

Next you will need the HFP events in a compressed csv file.
Review `testdata/import/hfp.csv.gz` for the expected format.
For the time being you have to prepare this data manually, since the tool does not yet include a direct integration to any HFP storage.
It is recommended to select the HFP events as follows:

- `DOO` and `DOC` events only: in these observations, the vehicles are very likely to be stopped and serving passengers (which should logically happen at real transit stops).
- Only those events with valid `long` & `lat` coordinates: `NULL`s or values clearly outside the HSL area are of no use here.
- Events from at least a week, or even a longer period, so stops with just a few daily trips get a decent sample size too.
- If you wish to analyse only a subset of stops, routes or areas, you can do it here by filtering the HFP input accordingly.

Gzip the csv file and save as `data/import/hfp.csv.gz` (relative to the project root), so the database will be able to read it through a mapped volume.
If there are rows already in the database conflicting with `(tst, event_type, oper, veh)`, they will be simply ignored from the csv import.

Now start the service stack, and once it is running successfully, invoke the data import and analysis scripts:

```
docker-compose up -d
docker-compose -f docker-compose.test.yml run --rm worker bash import_all.sh
docker-compose -f docker-compose.test.yml run --rm worker python run_analysis.py
```

Next you have to do some manual work.
Open the `qgis/stopcorr.qgs` project, and adjust the Postgres connection parameters, if needed, so the layers can connect to the database.
Export two sets of Atlas png images to `qgis/out/`: one using the `main` print layout, and another using the `index` print layout.
The files must be named like `main_<stop_id>.png` and `index_<stop_id>.png` so the `make_report.py` script finds them from the mapped volume with correct names.

Once ready with the main and index map images, run the reporter script:

```
docker-compose -f docker-compose.test.yml run --rm worker python make_report.py
```

Once the script has completed, you should find the result pptx file from the Python file server (change the port according to your `FILESERVER_HOST_PORT`):

```
http://localhost:8080/
```

### Database

The Postgres db is created automatically on container startup by running the SQL scripts mapped to the `docker-entrypoint-initdb.d` in alphabetical order.
However, this is NOT done if the database volume already exists and there is a database.
Either run new files manually in `psql`, or delete the volume to allow the db to be created from scratch.

When the server `docker-compose` services are running, the database should be reachable from the host machine through `localhost`, `PG_HOST_PORT` port and `PG_PASSWORD`.
The database name is `stopcorr` and username `postgres`.

### Adjusting map styles in QGIS

Adjusting the basemap and vector layers and their styles is done in the `qgis/stopcorr.qgs` project file.
The two print layouts, `main` and `index`, are used to produce Atlas png images from each median stop.
To adjust the layouts, open the layout, activate the map item and Atlas preview, and uncheck `Lock layers`.
Now the layer styles will follow the main window.
Finally, remember to check `Lock layers` again.

**NOTE:** The Postgres connection credentials are currently saved to the QGIS project file, which is unsafe and NOT recommended at least if the database is made available outside the host machine in any way.

### Adjusting the PowerPoint report

The PowerPoint report is created using the `python/src/pptx_template.pptx` file.
There you can adjust placeholder sizes, font styles etc. by editing the master slide template.
Note that each placeholder has an index that has been hard-coded to the `make_report.py` script;
if you add or remove placeholders, run `analyze_pptx()` from `stopcorr.utils` to check the new index numbers and update the script accordingly.

## Planned features

- Transfer development & maintenance of PostGIS db and procedures to InfoDevOps team (LAR retains the visualization & reporting stuff for the moment)
- Access results through a REST API
  - List all stops and key result parameters, such as Jore->median distance and percentile radius values
  - Get details and related observations of a given stop
- Trigger data updates and analyses through a REST API? (Authenticated user)
- Use date range versioned stops instead of a snapshot from single date
  - Allow using HFP samples based on the validity time of a selected stop for analysis

