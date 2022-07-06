# Analysis features

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