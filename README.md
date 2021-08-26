# stop-correspondence

This tool produces a report of HSL transit stops whose geographical position and/or stop detection radius may require adjustments.
By using the report, HSL personnel can adjust the coordinates and parameters of Jore stops to better reflect the real world, improving on the accuracy of stop area detection of transit vehicles in operation (and thus the operation data quality) as well as passenger information in Reittiopas and other info channels.

The analysis is made with the current stop data from Jore (via Digitransit; the same data as what's used in Reittiopas) and a sample of HFP events.
Results are combined into a PowerPoint file that can be further distributed, modified and commented on.

The stop-HFP correspondence analysis is a one-off task run a few times per year and requires some effort every time.

## Usage & development

### Deployment

The tool requires [docker-compose](https://docs.docker.com/compose/) and is run with two files: one with continuous services (database, QGIS map server and report file server) and another that will run the one-off tasks required for an analysis (HFP & Digitransit integrations, creating the pptx file).

Clone this project, and navigate to the project root. Copy the `.env.test` file into `.env`, and update the environment variables such as ports and the database password as you wish.

Create a common network for the docker-compose services:

```
docker network create --attachable stopcorr_nw
```

Update the environment variables for your session, and start the database & server stack:

```
source .env && docker-compose -f docker-compose.server.yml up -d
```

Now you should be able to connect to the (empty) file server at `http://localhost:8080/` (or whichever `FILESERVER_HOST_PORT` you set).

Next you will need the HFP events in a compressed csv file.
For the time being you have to prepare this data manually, since the tool does not yet include a direct integration to any HFP storage.
It is recommended to select the HFP events as follows:

- `DOO` and `DOC` events only: in these observations, the vehicles are very likely to be stopped and serving passengers (which should logically happen at real transit stops).
- Only those events with valid `long` & `lat` coordinates: `NULL`s or values clearly outside the HSL area are of no use here.
- Events from at least a week, or even a longer period, so stops with just a few daily trips get a decent sample size too.
- If you wish to analyse only a subset of stops, routes or areas, you can do it here by filtering the HFP input accordingly.

The csv file should look like this:

```
tst,event,oper,veh,route,dir,oday,start,stop_id,long,lat
# TODO: Example values
```

Gzip it and save as `data/db_import/hfp.csv.gz` (relative to the project root), so the database will be able to read it through a mapped volume.
If there are values already in the database conflicting with `(tst, event_type, oper, veh)`, they will be simply ignored from the csv import.

Start the data import and analysis stack:

```
docker-compose -f docker-compose.analysis.yml up
```

Watch the log messages - the data import, analysis as well as building the report will all take some time.
After all the services have exited successfully, you should be able to download the PowerPoint report named after the `ANALYSIS_NAME` env variable, e.g.:

```
http://localhost:8080/test_analysis.pptx
```

### Development

The tool is created using PostgreSQL 13 & PostGIS 3.0, Python 3.7, QGIS 3.16 and docker-compose 1.29.0.

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

The PowerPoint report is created using the `pptx/template.pptx` template.
There you can adjust placeholder sizes, font styles etc., however be careful not to delete any placeholders needed to create and populate the actual slides.

## Roadmap

Planned until the end of 2021 (along with another analysis round):

- Filtering of stops, terminals, routes and areas to analyse
- Adjusting parameters, such as minimum sample size per stop, outside the source code, e.g. in `.env`

Planned during 2022:

- Direct import of HFP events at a date range from HSL-DW (when HFP available there)
- Transfer deployment & maintenance of the tool from LAR-datatiimi to TRO
- Direct import of stops from Jore4, possibility to select a stop snapshot from a date in the past, rather than the current situation

## Contact

Arttu Kosonen, LAR-datatiimi
arttu.kosonen (at) hsl.fi
