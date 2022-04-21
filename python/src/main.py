from fastapi import FastAPI
from subprocess import call
from stopcorr.utils import get_conn_params
from stopcorr.utils import get_geojson_point
from run_analysis import main
import psycopg
import json

app = FastAPI()

@app.get("/")
async def root():
    return "Pysäkkianalyysi API:n root. Katso API:n dokumentaatio menemällä /docs tai /redoc"

@app.get("/run_import")
async def run_import():
    print("Running import...")
    call("./import_all.sh")
    # TODO: update /job_status here?
    return "Import done."

@app.get("/run_analysis")
async def run_analysis():
    print("Running analysis...")
    main()
    # TODO: update /job_status here?
    return "Analysis done."


@app.get("/jore_stops")
async def get_jore_stops(stop_id = -1):
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT row_to_json(row) FROM (SELECT * FROM jore_stop) row")
            stops = cur.fetchall()

            print(f'Found {len(stops)} Jore stops.')

            if len(stops) == 0:
                return 'Error: no stop data available. Have you ran Jore & HFP data imports and then analysis?'

            stopGeoJSONFeatures = []

            if stop_id != -1:
                stops = list(filter(lambda item: str(item[0]['stop_id']) == stop_id, stops))

                if len(stops) == 0:
                    return f'Did not find stop with given stop_id: {stop_id}'

            for stopTuple in stops:
                stop = stopTuple[0]
                stopFeature = get_geojson_point([stop['lat'], stop['long']], dict(
                    stop_id=stop['stop_id'],
                    stop_code=stop['stop_code'],
                    stop_name=stop['stop_name'],
                    parent_station=stop['parent_station'],
                    stop_mode=stop['stop_mode'],
                    route_dirs_via_stop=stop['route_dirs_via_stop'],
                    date_imported=stop['date_imported']
                ))
                stopGeoJSONFeatures.append(stopFeature)

    return stopGeoJSONFeatures


@app.get("/stop_medians")
async def get_stop_medians(stop_id = -1):
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            print(f'QUERY WITH {stop_id}')
            cur.execute("SELECT \
                json_build_object( \
                    'stop_id', sm.stop_id, \
                    'from_date', sm.from_date, \
                    'to_date', sm.to_date, \
                    'n_stop_known', sm.n_stop_known, \
                    'n_stop_guessed', sm.n_stop_guessed, \
                    'n_stop_null_near', sm.n_stop_null_near, \
                    'dist_to_jore_point_m', sm.dist_to_jore_point_m, \
                    'observation_route_dirs', sm.observation_route_dirs, \
                    'result_class', sm.result_class, \
                    'recommended_min_radius_m', sm.recommended_min_radius_m, \
                    'manual_acceptance_needed', sm.manual_acceptance_needed, \
                    'geom', sm.geom, \
                    'percentile_radii', json_build_object( \
                        'stop_id', pr.stop_id, \
                        'percentile', pr.percentile, \
                        'radius_m', pr.radius_m, \
                        'n_observations', pr.n_observations \
                    ) \
                ) \
            FROM stop_median sm \
            LEFT JOIN percentile_radii pr ON sm.stop_id = pr.stop_id"
            )

            stopMedians = cur.fetchall()

            print(f'Found {len(stopMedians)} Jore stop medians.')

            if len(stopMedians) == 0:
                return 'Error: no stop median data available. Have you ran Jore & HFP data imports and then analysis?'

            if stop_id != -1:
                stopMedians = list(filter(lambda item: str(item[0]['stop_id']) == stop_id, stopMedians))
                if len(stopMedians) == 0:
                    return f'Did not find stop median with given stop_id: {stop_id}'

            stopMedianDictionary = dict()

            for stopMedianTuple in stopMedians:
                stopMedian = stopMedianTuple[0]
                currentKey = stopMedian['stop_id']
                currentStopMedian = stopMedianDictionary.get(currentKey)
                if currentStopMedian is None:
                    currentStopMedian = get_geojson_point(stopMedian['geom']['coordinates'], dict(
                        stop_id=stopMedian['stop_id'],
                        from_date=stopMedian['from_date'],
                        to_date=stopMedian['to_date'],
                        n_stop_known=stopMedian['n_stop_known'],
                        n_stop_guessed=stopMedian['n_stop_guessed'],
                        n_stop_null_near=stopMedian['n_stop_null_near'],
                        dist_to_jore_point_m=stopMedian['dist_to_jore_point_m'],
                        observation_route_dirs=stopMedian['observation_route_dirs'],
                        result_class=stopMedian['result_class'],
                        recommended_min_radius_m=stopMedian['recommended_min_radius_m'],
                        manual_acceptance_needed=stopMedian['manual_acceptance_needed'],
                        percentile_radii_list=[dict(
                            percentile=stopMedian['percentile_radii']['percentile'],
                            radius_m=stopMedian['percentile_radii']['radius_m'],
                            n_observations=stopMedian['percentile_radii']['n_observations']
                        )]
                    ))
                    stopMedianDictionary[currentKey] = currentStopMedian
                else:
                    currentStopMedian['properties']['percentile_radii_list'].append(dict(
                        percentile=stopMedian['percentile_radii']['percentile'],
                        radius_m=stopMedian['percentile_radii']['radius_m'],
                        n_observations=stopMedian['percentile_radii']['n_observations']
                    ))
                    stopMedianDictionary[currentKey] = currentStopMedian

            stopMedianGeoJSONFeatures = []

            for key in stopMedianDictionary:
                stopMedianGeoJSONFeatures.append(stopMedianDictionary[key])

    return stopMedianGeoJSONFeatures


# Returns a GeoJSON with HFP (door) observations which were used for analysis of that stop_id
# OR which have NULL stop_id value but are located max nullStopIdDistanceInMeters around the stop
@app.get("/hfp_points/{stop_id}")
async def get_hfp_points(stop_id: str):
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute(f'SELECT row_to_json(row) FROM ( \
                SELECT * FROM \
                observation \
                WHERE stop_id = {stop_id}) row')
            observations = cur.fetchall()

            nullStopIdDistanceInMeters = 100
            cur.execute(f'SELECT row_to_json(row) FROM ( \
                WITH null_stop_ids_within_meters AS ( \
                SELECT \
                    found_ob_stops.lat, \
                    found_ob_stops.long, \
                    found_ob_stops.stop_id, \
                    found_ob_stops.stop_id_guessed, \
                    found_ob_stops.event, \
                    found_ob_stops.dist_to_jore_point_m, \
                    found_ob_stops.dist_to_median_point_m \
                FROM jore_stop js \
                INNER JOIN LATERAL ( \
                    SELECT * FROM observation AS ob \
                    WHERE ob.stop_id IS NULL AND \
                    ST_DWithin(ob.geom, js.geom, {nullStopIdDistanceInMeters}) \
                ) as found_ob_stops \
                ON true \
                WHERE js.stop_id = {stop_id} \
                ) \
                SELECT * FROM null_stop_ids_within_meters \
            ) row')
            nullStopIdObservations = cur.fetchall()
            print(nullStopIdObservations)
            observations = observations + nullStopIdObservations

            print(f'Found {len(observations)} observations.')

            if len(observations) == 0:
                return f'Did not find hfp data for given stop: {stop_id}'

            hfpGeoJSONFeatures = []

            for observationTuple in observations:
                observation = observationTuple[0]
                hfpFeature = get_geojson_point([observation['lat'], observation['long']], dict(
                    stop_id=observation['stop_id'],
                    stop_id_guessed=observation['stop_id_guessed'],
                    event=observation['event'],
                    dist_to_jore_point_m=observation['dist_to_jore_point_m'],
                    dist_to_median_point_m=observation['dist_to_median_point_m']
                ))
                hfpGeoJSONFeatures.append(hfpFeature)

    return hfpGeoJSONFeatures
