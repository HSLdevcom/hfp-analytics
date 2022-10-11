#!/bin/bash

set -eu

###########################################################################################################################
# Run this script to init a freshly created database with SQL files found from /db/sql                                    #
# To run this script, you need to give 4 parameters like so:                                                              #
# ./init_remote_database <db_password> hfp-analytics-<environment>-db.postgres.database.azure.com <db_name> <db_username> #
###########################################################################################################################

echo "Going to init database with sql files from /db/sql..."

for i in $(ls -l ../db/sql/*.sql |awk '{print $NF}'); do
    PGPASSWORD=$1 psql -h $2 -p 5432 -d $3 -U $4 -f $i;
done

echo "Done with init database."
