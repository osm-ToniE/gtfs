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
        error_code=0

        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Update GTFS feed '$feed'"

        subdir=$(echo $feed | sed -e 's/-/\//' -e 's/-/\//')

        if [ ! -d $GTFS_FEEDS_LOC/$subdir ]
        then
            subdir=$(echo $feed | sed -e 's/-/\//')
        fi

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
                ret_code=$?
                error_code=$(( $error_code + $ret_code ))

                if [ $error_code -eq 0 ]
                then
                    # publish as new
                    gtfs-feed.sh -Pn
                    ret_code=$?
                    error_code=$(( $error_code + $ret_code ))
                fi

                # clean, erase empty, wipe out old
                gtfs-feed.sh -cEW
                ret_code=$?
                error_code=$(( $error_code + $ret_code ))
            else
                # clean, download and analyze
                gtfs-feed.sh -ca    >  $LOGFILE 2>&1
                ret_code=$?
                error_code=$(( $error_code + $ret_code ))

                if [ $error_code -eq 0 ]
                then
                    # publish as new
                    gtfs-feed.sh -Pn    >> $LOGFILE 2>&1
                    ret_code=$?
                    error_code=$(( $error_code + $ret_code ))
                fi

                # clean, erase empty, wipe out old
                gtfs-feed.sh -cEW   >> $LOGFILE 2>&1
                ret_code=$?
                error_code=$(( $error_code + $ret_code ))
            fi
            LOGFILE=""
        else
            echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$feed : directory '$GTFS_FEEDS_LOC/$subdir' does not exist"
        fi
        if [ $error_code -gt 0 ]
        then
            echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Update GTFS feed '$feed' failed, see the logs"
        else
            echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Update GTFS feed '$feed' done"
        fi
    done

    echo ""
    echo "List of PTNA analysis configs where 'gtfs:release_date' is used in relations or CSV"
    echo ""

    # find all occurances of a GTFS feed in all *-Analysis.html files having 'release_date' set
    grep -r ', GTFS-Release-Date: 20' /osm/ptna/www/results/*/*Analysis.html |  \
    sed  -e 's/\(20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]\).*$/\1/'                   \
         -e 's/^.*data-ref="//'                                                 \
         -e 's/^.*GTFS-Feed: //'                                                \
         -e 's/, GTFS-Release-Date: /-/'                                        \
         -e 's/^.*feed=//'                                                      \
         -e 's/&release_date=/-/'                                             | \
    grep -v "gtfs:release_date"                                               | \
    sort -u

    cd $WHERE_AM_I
else
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "File '$FROM_FILE' not found or cannot be read"
    exit 3
fi
