#!/bin/bash

WHERE_AM_I=$PWD

for f in $(awk '/^[A-Z].*not yet analyzed/ { print $1; }' ~/tmp/gtfs-all-feeds.log)
do
    echo "#####################################"
    echo ""
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$f"
    echo ""
    echo "#####################################"

    subdir=$(echo $f | sed -e 's/-/\//' -e 's/-/\//')

    if [ -d $GTFS_FEEDS_LOC/$subdir ]
    then
        cd $GTFS_FEEDS_LOC/$subdir

        # clean, download and analyze
        gtfs-feed.sh -ca

        # publish as new
        gtfs-feed.sh -Pn

        # clean, erase empty, wipe out old
        gtfs-feed.sh -cEW
    fi
done

cd $WHERE_AM_I
