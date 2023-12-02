#!/bin/bash

#
# the GTFS-zip file should be store in a location like this:
#
# $PWD ends like this ... gtfs-network/DE/BY/MVV/2020-03-17"
#
# where DE-BY-MVV later on build the 'network' part of the target DB file
# where DE and BY and MVV later on build part of the target path /osm/ptna/work/DE/BY/DE-BY-MVV-ptna-gtfs-sqlite.db
# where we expect two files from former analysis to be in the parent directory, i.e.
# ../osm.txt
# ../ptna.txt
#

use_language="de"

network_dir=$(dirname "$PWD")

#D3=$(basename "$network_dir")

D2_path=$(dirname "$network_dir")
D2=$(basename "$D2_path")

D1_path=$(dirname "$D2_path")
D1=$(basename "$D1_path")

if [ "$D1" = "gtfs-networks" ]
then
#    TARGET_DB="$WORK_BASE_DIR/$D2/$D2-$D3-$DB"
#    FORMER_DB="$WORK_BASE_DIR/$D2/$D2-$D3-prev-$DB"
    COUNTRY=$D2
else
#    TARGET_DB="$WORK_BASE_DIR/$D1/$D2/$D1-$D2-$D3-$DB"
#    FORMER_DB="$WORK_BASE_DIR/$D1/$D2/$D1-$D2-$D3-prev-$DB"
    COUNTRY=$D1
fi

if [ "$COUNTRY" = "DE" ]
then
    use_language="de"
elif [ "$COUNTRY" = "CH" ]
then
    use_language="de_CH"
elif [ "$COUNTRY" = "AT" ]
then
    use_language="de_AT"
else
    use_language="en"
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') start preparation $*"
gtfs-prepare-ptna-sqlite.sh --language=$use_language "$*"

echo "$(date '+%Y-%m-%d %H:%M:%S') start aggregation $*"
gtfs-aggregate-ptna-sqlite.pl --language=$use_language "$*"

echo "$(date '+%Y-%m-%d %H:%M:%S') start analysis $*"
gtfs-analyze-ptna-sqlite.pl --language=$use_language "$*"

echo "$(date '+%Y-%m-%d %H:%M:%S') start normalization $*"
gtfs-normalize-ptna-sqlite.pl --language=$use_language "$*"

if [ -f ../post-process-ptna-sqlite.sh ]
then
    echo "$(date '+%Y-%m-%d %H:%M:%S') start post processing $*"
    ../post-process-ptna-sqlite.sh
fi
