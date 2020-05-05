#!/bin/bash

#
# ths GTFS-zip file should be store in a location like this:
#
# $PWD ends like this ... DE/BY/MVV/2020-03-17"
#
# where DE-BY-MVV later on build the 'network' part of the target DB file
# where DE and BY and MVV later on build part of the target path /osm/ptna/work/DE/BY/DE-BY-MVV-ptna-gtfs-sqlite.db
# where we expect two files from former analysis to be in the parent directory, i.e.
# ../osm.txt
# ../ptna.txt
#

DB="ptna-gtfs-sqlite.db"

WORK_BASE_DIR="/osm/ptna/work"

release_date=$(basename $PWD)

if [ $(echo $release_date | grep -c '\d\d\d\d-\d\d-\d\d') eq 0 ]
then
    release_date=$(date '%Y-%m-%d')
fi

network_dir=$(dirname $PWD)

D3=$(basename $network_dir)

D2_path=$(dirname $network_dir)
D2=$(basename $D2_path)

D1_path=$(dirname $D2_path)
D1=$(basename $D1_path)

if [ "$D1" = "gtfs-networks" ]
then
    TARGET_DB="$WORK_BASE_DIR/$D2/$D2-$D3-$DB"
    echo $TARGET_DB

    if [ "$D2" != "DE" ]
    then
        ANALYSIS_LANG="--language=en"
    fi
else
    TARGET_DB="$WORK_BASE_DIR/$D1/$D2/$D1-$D2-$D3-$DB"
    echo $TARGET_DB

    if [ "$D1" != "DE" ]
    then
        ANALYSIS_LANG="--language=en"
    fi
fi


echo $(date '+%Y-%m-%d %H:%M:%S') "start preparation"
gtfs-prepare-ptna-sqlite.sh $*

echo $(date '+%Y-%m-%d %H:%M:%S') "start aggregation"
gtfs-aggregate-ptna-sqlite.pl $*

echo $(date '+%Y-%m-%d %H:%M:%S') "start analysis $ANALYSIS_LANG"
gtfs-analyze-ptna-sqlite.pl $ANALYSIS_LANG $*

echo $(date '+%Y-%m-%d %H:%M:%S') "start normalization $ANALYSIS_LANG "
gtfs-normalize-ptna-sqlite.pl $ANALYSIS_LANG $*

echo $(date '+%Y-%m-%d %H:%M:%S') "update release_date = $release_date"
sqlite3 -header -csv $DB "update ptna set release_date='$release_date' where id=1;"

echo $(date '+%Y-%m-%d %H:%M:%S') "start rsync -rtvu $DB $TARGET_DB"
rsync -rtvu $DB $TARGET_DB
