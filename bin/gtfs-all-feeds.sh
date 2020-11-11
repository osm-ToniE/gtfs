#!/bin/bash

#
# analyze GTFS feeds
#


cd $GTFS_FEEDS_LOC

WD=$PWD

for S in $(find . -name ptna.txt | sort)
do
    D=$(dirname $S)
    if [ -d $D ]
    then
        #echo
        #echo $(date "+%Y-%m-%d %H:%M:%S") "$D"
        #echo

        cd $D

        gtfs-feed.sh $*

        cd $WD
    fi

done
