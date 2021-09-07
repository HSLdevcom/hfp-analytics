# stop-correspondence

This tool produces a report of HSL transit stops whose geographical position and/or stop detection radius may require adjustments.
By using the report, HSL personnel can adjust the coordinates and parameters of Jore stops to better reflect the real world, improving on the accuracy of stop area detection of transit vehicles in operation (and thus the operation data quality) as well as passenger information in Reittiopas and other info channels.

The analysis is made with the current stop data from Jore (via Digitransit; the same data as what's used in Reittiopas) and a sample of HFP events.
Results are combined into a PowerPoint file that can be further distributed, modified and commented on.

The stop-HFP correspondence analysis is a one-off task run a few times per year and requires some manual effort every time.

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

The map images are generated using the [QGIS Server](https://docs.qgis.org/3.16/en/docs/server_manual/index.html) WMS service, which uses a normal QGIS project file, which in turn uses the Postgres database for all the vector data.
Adjusting the basemap and vector layers and their styles is done in the `qgis/stopcorr.qgs` project file.
For this you will need QGIS Desktop >= 3.16.
Note that the vector layers are configured to use the database connection parameters within the container network and will therefore not work directly when you open the project on your host machine.

**NOTE:** The Postgres connection credentials are currently saved to the QGIS project file, which is unsafe and NOT recommended at least if the database is made available outside the host machine in any way.

### Adjusting the PowerPoint report

The PowerPoint report is created using the `python/src/pptx_template.pptx` file.
There you can adjust placeholder sizes, font styles etc. by editing the master slide template.
Note that each placeholder has an index that has been hard-coded to the `make_report.py` script;
if you add or remove placeholders, run `analyze_pptx()` from `stopcorr.utils` to check the new index numbers and update the script accordingly.

## Roadmap

Planned until the end of 2021 (along with another analysis round):

- Remove the blank first slide from the result pptx
- Filtering of stops, terminals, routes and areas to analyse
- Create / amend an analysis for one or multiple selected stops
- Create a report from selected stops
- Automate main and index map using e.g. QGIS Server & Atlas plugin or Geopandas & map plotting libs

Planned during 2022:

- Serve per-stop results through an API
- Access per-stop maps interactively from the browser
- Adjust analysis parameters and re-run the analysis for a selected stop from the browser
- Direct import of HFP events at a date range from HSL-DW (when HFP available there)
- Transfer deployment & maintenance of the tool from LAR-datatiimi to TRO
- Direct import of stops from Jore4, possibility to select a stop snapshot from a date in the past, rather than the current situation

## Contact

Arttu Kosonen, LAR-datatiimi
arttu.kosonen (at) hsl.fi
