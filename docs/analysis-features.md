# Analysis features

## Geographical area analysis

GPS data quality can vary due to tunnels, underground terminals or other problematic areas.

- Are there geographical areas with significant quality issues, such as positioning errors or message transmission delays?
- Do the issues in an area change over time?

## Journey availability analysis

A scheduled journey (route, dir, oday, start) exists in the HFP data, if there is at least one event from it.
Jore is considered the master data of scheduled journeys, HFP in turn is the master source for actual journeys.

- Which scheduled journeys are found in the HFP data?
- Which journeys are found in the HFP data but not in scheduled journeys?
- Which scheduled journeys were realized more than once?
  - Were they valid duplicated journeys or e.g. used as test cases (large difference between planned and actual times)?

## Journey route validity analysis

Sometimes a vehicle serves a valid journey and route while it's signed in to a completely wrong journey with a different route.
A journey route can also be only partially fulfilled according to HFP data, or have gaps.
These could be spotted by comparing the planned Jore route geometry and actual GPS trace with `ST_FrechetDistance()`, for example.
The function returns a "similarity" value that could distinguish between *approximately* similar Jore and GPS tracks vs. those that differ significantly.

- Report journeys where the planned and actual geometry differs suspiciously much. Trigger an automatic alert and/or analyze them in more detail manually.

## Raw data availability

HFP-Analytics ingests data from flat files that are collected from the realtime API.
Data can be missing due to realtime errors or storage errors.

- Flat files by event type, their row counts, and `tst` ranges available from a given datetime range.
- Unexpected gaps in raw data availability, e.g. compared to typical row counts of the hour, day of week, and event type.
- Current data available in the HFP-Analytics db. E.g., row count by quarter hour.

## Stop correspondence analysis

This feature is used to report HSL transit stops whose geographical **position and/or stop detection radius** may require adjustments in Jore, to better reflect the real-world position and area where the vehicles stop.
This way, realtime systems are able to provide actual arrival and departure time observations of vehicles at stops as accurately as possible.
If a stop point is modeled too far away from the GPS point cloud of actual stopping locations, or if the detection radius is too small, we might get a lot of missing arr/dep times;
if the detection radius is too large, then the arrival times are often too early and departure times too late.

The analysis procedure and related tables in the `stopcorr` db schema are described below.

![Stop correspondence analysis phases.](img/stopcorr-relations-and-procedures.png)

The analysis is made with the current stop data from Jore (via Digitransit; the same data as what's used in Reittiopas) and a sample of HFP door events.
HFP-Analytics imports the data and runs the analysis regularly.
The user can then fetch the results from the API for reporting.

> **TO DO: Reporting tutorial**.
> 
> Results are combined into a PowerPoint file that can be further distributed, modified and commented on.
> 
> The stop-HFP correspondence analysis is a one-off task run a few times per year and requires some manual effort every time.

## Vehicle analysis

HFP data quality issues are often related to faulty sensors or devices in a certain vehicle.
Therefore we'd like to monitor at least the following things by vehicle and date, for example:

- Missing or inverted door status `drst`.
- Missing or faulty odometer readings `odo`. E.g., the values may be negative, change too fast or slowly compared to real distance traveled, advance in large steps only, or reset randomly to zero.
- Amount of GPS jitter, especially when the vehicle does not move.
- Journeys that the vehicle was signed in to, and their temporal coverage (e.g., spot accidental sign-ins to a wrong journey).
- Missing or excess vehicles in HFP data, compared to Jore fleet registry:
  - Operator + vehicle id combinations in HFP but not in the registry?
  - Vehicles that haven't sent HFP messages for a long time although they should?
