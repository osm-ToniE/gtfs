#!/bin/bash

#
# analyze GTFS feeds
#


cd "$GTFS_FEEDS_LOC" || { echo "cannot cd into GTFS_FEEDS_LOC \"$GTFS_FEEDS_LOC\""; exit 1; }

WD=$PWD

for S in $(find . -name ptna.txt | sort)
do
    D=$(dirname "$S")
    if [ -d "$D" ]
    then
        #echo
        #echo "$(date '+%Y-%m-%d %H:%M:%S') $D"
        #echo

        cd "$D" || { echo "cannot cd into D \"$D\""; exit 1; }

        if [ -f "get-feed-name.sh" ] && [ -f "get-release-url.sh" ] && [ -f "get-release-date.sh" ] && [ -f "cleanup.sh" ] && [ -f "osm.txt" ]
        then
            gtfs-feed.sh $*
        else
            echo "$D: at least one missing file: \"get-feed-name.sh\", \"get-release-url.sh\", \"get-release-date.sh\", \"cleanup.sh\", \"osm.txt\""
        fi

        cd "$WD" || { echo "cannot cd into WD \"$WD\""; exit 1; }
    fi

done
