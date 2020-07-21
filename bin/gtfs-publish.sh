#!/bin/bash

#
# the GTFS-zip file should be store in a location like this:
#
# $PWD ends like this ... gtfs-network/DE/BY/MVV/2020-03-17"
#
# where DE-BY-MVV later on build the 'network' part of the target DB file
# where DE and BY and MVV later on build part of the target path /osm/ptna/work/DE/BY/DE-BY-MVV-ptna-gtfs-sqlite.db
#

DB="ptna-gtfs-sqlite.db"

WORK_BASE_DIR="/osm/ptna/work"

network_dir=$(dirname $PWD)

D3=$(basename $network_dir)

D2_path=$(dirname $network_dir)
D2=$(basename $D2_path)

D1_path=$(dirname $D2_path)
D1=$(basename $D1_path)

if [ "$D1" = "gtfs-networks" ]
then
    TARGET_DB="$WORK_BASE_DIR/$D2/$D2-$D3-$DB"
    FORMER_DB="$WORK_BASE_DIR/$D2/$D2-$D3-prev-$DB"
    COUNTRY=$D2
else
    TARGET_DB="$WORK_BASE_DIR/$D1/$D2/$D1-$D2-$D3-$DB"
    FORMER_DB="$WORK_BASE_DIR/$D1/$D2/$D1-$D2-$D3-prev-$DB"
    COUNTRY=$D1
fi

if [ "$COUNTRY" = "DE" -o "$COUNTRY" = "CH" ]
then
    use_language=de
else
    use_language=en
fi

if [ -f $TARGET_DB ]
then
    rm -f previous.db

    echo $(date '+%Y-%m-%d %H:%M:%S') "start rsync -tvu $TARGET_DB previous.db"
    rsync -tvu $TARGET_DB previous.db

    old_release_date=$(sqlite3 previous.db "SELECT release_date FROM ptna WHERE id=1;")
    echo $(date '+%Y-%m-%d %H:%M:%S') "Release date of former DB: $old_release_date"

    if [ "$use_language" = "de" ]
    then
        new_comment="Dieses ist eine ältere Version der GTFS-Daten: $old_release_date"
    else
        new_comment="This is an older version of the GTFS data: $old_release_date"
    fi

    sqlite3 previous.db "UPDATE ptna set comment='$new_comment' WHERE id=1;"

    sqlite3 previous.db "SELECT comment FROM ptna WHERE id=1;"

    echo $(date '+%Y-%m-%d %H:%M:%S') "start rsync -tvu previous.db $FORMER_DB"
    rsync -tvu previous.db $FORMER_DB
fi

echo $(date '+%Y-%m-%d %H:%M:%S') "start rsync -tvu $DB $TARGET_DB"
rsync -tvu $DB $TARGET_DB
