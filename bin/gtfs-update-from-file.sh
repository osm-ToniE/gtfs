#!/bin/bash

FROM_FILE="$HOME/tmp/gtfs-all-feeds.log"

TEMP=$(getopt -o f:l --long file:,log-separate -n 'gtfs-update-from-file.sh' -- "$@")

if [ $? != 0 ] ; then echo "Terminating..." ; exit 1 ; fi

eval set -- "$TEMP"

while true ; do
    case "$1" in
        -f|--file)                  FROM_FILE=$2        ; shift 2 ;;
        -l|--log-separate)          LOG_SEPARATE=true   ; shift   ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 2 ;;
    esac
done

if [ -f "$FROM_FILE" -a -r "$FROM_FILE" ]
then
    WHERE_AM_I=$PWD

    for feed in $(awk '/^[A-Z].*not yet analyzed/ { print $1; }' "$FROM_FILE")
    do
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$feed"

        subdir=$(echo $feed | sed -e 's/-/\//' -e 's/-/\//')

        if [ -d $GTFS_FEEDS_LOC/$subdir ]
        then
            #
            # redirect logging to $PREFIX.log file
            #

            if [ "$LOG_SEPARATE" = "true" ]
            then
                logdir=$(echo $feed | sed -e 's/-/\//' -e 's/-.*$/\//')
                if [ -d "$logdir" ]
                then
                    exec 1> $PTNA_WORK_LOC/$logdir/$feed-gtfs-update.log 2>&1
                else
                    logdir=$(echo $feed | sed -e 's/-.*$/\//')
                    if [ -d "$logdir" ]
                    then
                        exec 1> $PTNA_WORK_LOC/$logdir/$feed-gtfs-update.log 2>&1
                    fi
                fi
            fi

            cd $GTFS_FEEDS_LOC/$subdir

            # clean, download and analyze
            gtfs-feed.sh -ca

            # publish as new
            gtfs-feed.sh -Pn

            # clean, erase empty, wipe out old
            gtfs-feed.sh -cEW

            if [ "$LOG_SEPARATE" = "true" ]
            then
                exec 1>> /dev/stdout 2>> /dev/stderr
            fi
        else
            echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$feed : directory '$GTFS_FEEDS_LOC/$subdir' does not exist"
        fi
    done

    cd $WHERE_AM_I
else
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "File '$FROM_FILE' not found or cannot be read"
    exit 3
fi
