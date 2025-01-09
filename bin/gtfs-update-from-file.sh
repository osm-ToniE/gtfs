#!/bin/bash

FROM_FILE="$HOME/tmp/gtfs-all-feeds-cDT.log"

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
            cd $GTFS_FEEDS_LOC/$subdir

            if [ "$LOG_SEPARATE" = "true" ]
            then
                #
                # redirect logging to $feed-gtfs-update.log file
                #

                logdir=$PTNA_WORK_LOC/$(echo $feed | sed -e 's/-/\//' -e 's/-.*$//')
                if [ -d "$logdir" ]
                then
                    LOGFILE=$logdir/$feed-gtfs-update.log
                else
                    logdir=$PTNA_WORK_LOC/$(echo $feed | sed -e 's/-.*$//')
                    if [ -d "$logdir" ]
                    then
                        LOGFILE=/$logdir/$feed-gtfs-update.log
                    fi
                fi
            fi
            if [ -z "$LOGFILE" ]
            then
                # clean, download and analyze
                gtfs-feed.sh -ca

                # publish as new
                gtfs-feed.sh -Pn

                # clean, erase empty, wipe out old
                gtfs-feed.sh -cEW
            else
                # clean, download and analyze
                gtfs-feed.sh -ca    >  $LOGFILE 2>&1

                # publish as new
                gtfs-feed.sh -Pn    >> $LOGFILE 2>&1

                # clean, erase empty, wipe out old
                gtfs-feed.sh -cEW   >> $LOGFILE 2>&1
            fi
            LOGFILE=""
        else
            echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$feed : directory '$GTFS_FEEDS_LOC/$subdir' does not exist"
        fi
    done

    cd $WHERE_AM_I
else
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "File '$FROM_FILE' not found or cannot be read"
    exit 3
fi
