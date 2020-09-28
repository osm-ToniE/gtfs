#!/bin/bash

#
# Analysis for a single GTFS feed
#
# execute this in the directory of the feed definition
# expected files: get_release_date.sh, get_release_url.sh, cleanup.sh
#

TEMP=$(getopt -o cdfuv --long clean,date,feed,url,verbose -n 'gtfs-feed.sh' -- "$@")

if [ $? != 0 ] ; then echo $(date "+%Y-%m-%d %H:%M:%S") "Terminating..."  >> /dev/stderr ; exit 2 ; fi

eval set -- "$TEMP"

while true ; do
    case "$1" in
        -c|--clean)          clean=true         ; shift ;;
        -d|--date)           print_date=true    ; shift ;;
        -f|--feed)           print_feed=true    ; shift ;;
        -u|--url)            print_url=true     ; shift ;;
        -v|--verbose)        verbose='-v'       ; shift ;;
        --) shift ; break ;;
        *) echo $(date "+%Y-%m-%d %H:%M:%S") "Internal error!" >> /dev/stderr ; exit 3 ;;
    esac
done

#
#
#

if [ "$clean" = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Removing temporary files" >> /dev/stderr
    ./cleanup.sh $verbose
    rm -rf 20[0-9][0-9]-[0-1][0-9]-[0-3][0-6]
fi

#
#
#

if [ "$print_date" = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Retrieving Release-Date" >> /dev/stderr
    ./get-release-date.sh $verbose
fi

#
#
#

if [ "$print_feed" = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Retrieving Feed Name" >> /dev/stderr
    ./get-feed-name.sh $verbose
fi

#
#
#

if [ "$print_url"  = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Retrieving Release-URL" >> /dev/stderr
    ./get-release-url.sh $verbose
fi
