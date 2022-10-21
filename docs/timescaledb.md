# TimescaleDB

### Why?

TimescaleDB was chosen because we are able to achieve [scalable query execution performance](https://docs.timescale.com/timescaledb/latest/overview/how-does-it-compare/timescaledb-vs-postgres/#much-higher-ingest-rates) when dealing with HFP-data due to TimescaleDB's ability to time-space partition HFP-data. Previously without TimescaleDB, time taken by stopcorr analysis grew exponentially; with 1 day it took 80 seconds, with 2 days it took 409 seconds and with 4 days it took 5,5 hours. With TimescaleDB we are able to analyze 7 days of HFP-data in just 944 seconds.


### Setup Azure to use Timescale DB

```
az login
az account set --subscription <subscription_id>
az postgres flexible-server parameter set --name azure.extensions --value postgis,timescaledb --resource-group <resource_group_name> --server-name <server_name>
```
As adding extensions doesn't currently work from Azure portal.

### Create a hypertable

You can transform for example table `hfp.hfp_point` as a hypertable with:
```
SELECT create_hypertable('hfp.hfp_point', 'point_timestamp', chunk_time_interval => INTERVAL '1 day');
```
Note that a table should be empty when transforming it to a hypertable.

### Change hypertable interval

How to [change interval](https://docs.timescale.com/timescaledb/latest/how-to-guides/hypertables/change-chunk-intervals/#change-the-chunk-interval-length-on-an-existing-hypertable):

```
SELECT set_chunk_time_interval('hfp.hfp_point', INTERVAL '1 day');
or
SELECT set_chunk_time_interval('hfp.hfp_point', INTERVAL '12 hours');
```
Note that the setting **only applies to new chunks**. Existing chunks will not get affected. If you want all data to have the new interval, you should remove existing HFP data with:
```
TRUNCATE hfp_point, observed_journey, observation;
```
and import it again.

### Check existing interval settings

```
SELECT h.table_name, c.interval_length
  FROM _timescaledb_catalog.dimension c
  JOIN _timescaledb_catalog.hypertable h
    ON h.id = c.hypertable_id;
```

### See chunk sizes

```
SELECT min(table_bytes) AS table_min_bytes, max(table_bytes) AS table_max_bytes, min(index_bytes) AS idx_min_bytes, max(index_bytes) AS idx_max_bytes FROM chunks_detailed_size('hfp.hfp_point');
```

### Benchmarking

**1 day interval**
- with 1 days HFP: 120s analysis
- with 2 days HFP: 204s analysis
- with 3 days HFP: 338s analysis
- with 7 days HFP: 944s analysis

**chunk sizes**
```
 table_min_bytes | table_max_bytes | idx_min_bytes | idx_max_bytes
-----------------+-----------------+---------------+---------------
          417792 |       105635840 |        466944 |      76963840
```

**12 hours interval**
- with 1 days HFP: 118 sec analysis
- with 2 days HFP: 213 sec analysis
- with 3 days HFP: 366 sec analysis

**chunk sizes**
```
 table_min_bytes | table_max_bytes | idx_min_bytes | idx_max_bytes
-----------------+-----------------+---------------+---------------
          417792 |        54296576 |        483328 |      43843584
```

**6 hours interval**
- with 1 days HFP: 123 sec analysis
- with 2 days HFP: 246 sec analysis
- with 3 days HFP: 331 sec analysis
- with 7 days HFP: 961 sec analysis

**chunk sizes**
```
 table_min_bytes | table_max_bytes | idx_min_bytes | idx_max_bytes
-----------------+-----------------+---------------+---------------
          417792 |        36003840 |        466944 |      30834688
```

**3 hours interval**
- with 1 days HFP: 108 sec analysis
- with 2 days HFP: 180 sec analysis
- with 3 days HFP: 396 sec analysis

**chunk sizes**
```
 table_min_bytes | table_max_bytes | idx_min_bytes | idx_max_bytes
-----------------+-----------------+---------------+---------------
          417792 |        22454272 |        475136 |      19357696
```




### Conclusion

As you can see, there is no clear interval winner for now. We should however, we should keep testing with larger amount (1-2 week) of HFP-data. Currently 1 day and 6 hour intervals seem to be the best choice.
