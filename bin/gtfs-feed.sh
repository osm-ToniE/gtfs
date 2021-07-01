#!/bin/bash

#
# Analysis for a single GTFS feed
#
# execute this in the directory of the feed definition
# expected files: get_release_date.sh, get_release_url.sh, cleanup.sh
#

TEMP=$(getopt -o acdDfnoPTuv --long analyze,clean,date-print,date-check,feed-print,new,old,publish,touch-non-existent,url-print,verbose -n 'gtfs-feed.sh' -- "$@")

if [ $? != 0 ] ; then echo $(date "+%Y-%m-%d %H:%M:%S") "Terminating..."  >> /dev/stderr ; exit 2 ; fi

eval set -- "$TEMP"

while true ; do
    case "$1" in
        -a|--analyzed)              analyze=true        ; shift ;;
        -c|--clean)                 clean=true          ; shift ;;
        -d|--date-print)            date_print=true     ; shift ;;
        -D|--date-check)            date_check=true     ; shift ;;
        -f|--feed)                  feed_print=true     ; shift ;;
        -n|--new)                   publish_as_new='-n' ; shift ;;
        -o|--old)                   publish_as_old='-o' ; shift ;;
        -P|--publish)               publish=true        ; shift ;;
        -T|--touch-non-existent)    touch_n_e=true      ; shift ;;
        -u|--url-print)             url_print=true      ; shift ;;
        -v|--verbose)               verbose='-v'        ; shift ;;
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
    if [ -f ./cleanup.sh ]
    then
        ./cleanup.sh $verbose
    fi
    rm -rf 20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]*
fi

#
#
#

if [ "$date_print" = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Retrieving Release-Date" >> /dev/stderr
    if [ -f ./get-release-date.sh ]
    then
        ./get-release-date.sh $verbose
    else
        echo manually >> /dev/stderr
    fi
fi

#
#
#

if [ "$date_check" = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Checking Release-Date against existing feeds" >> /dev/stderr
    if [ -f ./get-release-date.sh -a -f ./get-feed-name.sh ]
    then
        RELEASE_DATE=$(./get-release-date.sh)
        FEED_NAME=$(./get-feed-name.sh)

        if [ -n "$RELEASE_DATE" -a -n "$FEED_NAME" ]
        then
            # on the web and in the work directory, the data will be stored in sub-directories
            # FEED_NAME=DE-BY-MVV --> stored in SUB_DIR=DE/BY
            # FEED_NAME=DE-BW-DING-SWU --> stored in SUB_DIR=DE/BW
            # FEED_NAME=DE-SPNV --> stored in SUB_DIR=DE

            # PREFIX=FR-IDF-entre-seine-et-foret --> changed into in SUB_DIR=FR/IDF-entre-seine-et-foret
            SUB_DIR=${FEED_NAME/-//}
            # SUB_DIR=FR/IDF-entre-seine-et-foret --> changed into in SUB_DIR=FR/IDF/entre-seine-et-foret
            SUB_DIR=${SUB_DIR/-//}
            # SUB_DIR=FR/IDF/entre-seine-et-foret --> changed into in SUB_DIR=FR/IDF
            SUB_DIR="${SUB_DIR%/*}"

            COUNTRY_DIR="${FEED_NAME%%-*}"

            if [ -f $PTNA_WORK_LOC/$COUNTRY_DIR/$FEED_NAME-ptna-gtfs-sqlite.db ]
            then
                WORK_LOC="$PTNA_WORK_LOC/$COUNTRY_DIR"
            elif [ -f $PTNA_WORK_LOC/$SUB_DIR/$FEED_NAME-ptna-gtfs-sqlite.db ]
            then
                WORK_LOC="$PTNA_WORK_LOC/$SUB_DIR"
            fi

            if [ -f $WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db ]
            then
                if [ -s $WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db ]
                then
                    echo $FEED_NAME - $RELEASE_DATE - OK
                else
                    echo $FEED_NAME - $RELEASE_DATE - empty file
                fi
            else
                echo $FEED_NAME - $RELEASE_DATE - not yet analyzed

                if [ "$touch_n_e" = "true" ]
                then
                    touch $WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db
                fi
            fi
        else
            if [ -z "$RELEASE_DATE" ]
            then
                echo "$FEED_NAME - unknown release date"
            else
                echo "$PWD - unknown feed name"
            fi
        fi
    else
        FEED_NAME=$(echo $PWD | sed -e "s|^$GTFS_FEEDS_LOC/||" -e 's|/|-|g')
        echo $FEED_NAME - manually >> /dev/stderr
    fi
fi

#
#
#

if [ "$feed_print" = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Retrieving Feed Name" >> /dev/stderr
    if [ -f ./get-feed-name.sh ]
    then
        ./get-feed-name.sh $verbose
    else
        echo manually >> /dev/stderr
    fi
fi

#
#
#

if [ "$url_print"  = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Retrieving Release-URL" >> /dev/stderr
    if [ -f ./get-release-url.sh ]
    then
        ./get-release-url.sh $verbose
    else
        echo manually >> /dev/stderr
    fi
fi

#
#
#

if [ "$analyze"  = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Analyzing GTFS package" >> /dev/stderr
    if [ -f ./get-release-url.sh -a -f ./get_release-date.sh ]
    then
        rd=$(./get-release-date.sh)
        ru=$(./get-release-url.sh)
        if [ -n "$rd" -a -n "$ru" ]
        then
            [ -d "$rd" ] || mkdir $rd
            cd $rd
            wget --user-agent "PTNA script on https://ptna.openstreetmap.de" -O gtfs.zip "$ru"
            gtfs-handle-zip.sh
            cd ..
        else
            echo failed >> /dev/stderr
        fi
    else
        echo manually >> /dev/stderr
    fi
fi

#
#
#

if [ "$publish"  = "true" ]
then
    [ -n "$verbose" ] && echo $(date "+%Y-%m-%d %H:%M:%S") "Publishing data" >> /dev/stderr
    rd=$(./get-release-date.sh)
    if [ -n "$rd" -a -d "$rd" ]
    then
        cd $rd
        gtfs-publish.sh $publish_as_new $publish_as_old
        cd ..
    else
        echo failed >> /dev/stderr
    fi
fi
