#!/bin/bash

#
# analyze GTFS feeds
#


cd "$GTFS_FEEDS_LOC" || { echo "cannot cd into GTFS_FEEDS_LOC $GTFS_FEEDS_LOC"; exit 1; }

WD=$PWD

for S in $(find . -name ptna.txt | sort)
do
    D=$(dirname "$S")
    if [ -d "$D" ]
    then
        #echo
        #echo $(date "+%Y-%m-%d %H:%M:%S") "$D"
        #echo

        cd "$D" || { echo "cannot cd into D \"$D\""; exit 1; }

        gtfs-feed.sh "$*"

        cd "$WD" || { echo "cannot cd into WD \"$WD\""; exit 1; }
    fi

done
