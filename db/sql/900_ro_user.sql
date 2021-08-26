-- Create a read-only user for QGIS server.
CREATE USER qgis_ro WITH LOGIN PASSWORD 'qgis_ro';

GRANT SELECT ON ALL TABLES IN SCHEMA public TO qgis_ro;
