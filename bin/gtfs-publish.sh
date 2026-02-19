#!/bin/bash

#
# the GTFS-zip file should be store in a location like this:
#
# $PWD ends like this ...
# - gtfs-network/DE/BY/MVV/2020-03-17
# or
# - gtfs-network/AT/VVV/2020-03-17
#
# where DE-BY-MVV later on build the 'network' part of the target DB file
# where DE and BY and MVV later on build part of the target path /osm/ptna/work/DE/BY/DE-BY-MVV-ptna-gtfs-sqlite.db
#

error_code=0

DB="ptna-gtfs-sqlite.db"

DB_DIR=$PWD

SQ_OPTIONS="-init /dev/null"

WORK_BASE_DIR="/osm/ptna/work"

GTFS_DIR=$PWD

if [ -f "$DB" ]
then
    RELEASE_DATE=$(sqlite3 $SQ_OPTIONS "$DB" "SELECT release_date FROM ptna WHERE id=1 LIMIT 1;")

    if [ -n "$RELEASE_DATE" ]
    then
        RELEASE_DATE=$(basename "$GTFS_DIR")
    fi

    network_dir=$(dirname "$GTFS_DIR")

    D3=$(basename "$network_dir")

    D2_path=$(dirname "$network_dir")
    D2=$(basename "$D2_path")

    D1_path=$(dirname "$D2_path")
    D1=$(basename "$D1_path")

    if [ "$D1" = "gtfs-feeds" ]
    then
        TARGET_DIR="$WORK_BASE_DIR/$D2"
        TARGET_SYM="$D2-$D3-$DB"
        PREVIOUS_SYM="$D2-$D3-previous-$DB"
        #LONGTERM_SYM="$D2-$D3-long-term-$DB"
        WITHDATE_DB="$D2-$D3-$RELEASE_DATE-$DB"
        ERROR_SYM="$D2-$D3-error-$DB"
        COUNTRY=$D2
    else
        TARGET_DIR="$WORK_BASE_DIR/$D1/$D2"
        TARGET_SYM="$D1-$D2-$D3-$DB"
        PREVIOUS_SYM="$D1-$D2-$D3-previous-$DB"
        #LONGTERM_SYM="$D1-$D2-$D3-long-term-$DB"
        WITHDATE_DB="$D1-$D2-$D3-$RELEASE_DATE-$DB"
        ERROR_SYM="$D1-$D2-$D3-error-$DB"
        COUNTRY=$D1
    fi

    if [ "$COUNTRY" = "DE" ] || [ "$COUNTRY" = "CH" ] || [ "$COUNTRY" = "AT" ]
    then
        use_language=de
    else
        use_language=en
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') Publish as with 'date' = $RELEASE_DATE"

    echo "$(date '+%Y-%m-%d %H:%M:%S') start rsync -tvu $DB $TARGET_DIR/$WITHDATE_DB"
    mkdir -p "$TARGET_DIR" 2> /dev/null
    rsync -tvu $DB "$TARGET_DIR/$WITHDATE_DB"

    cd "$TARGET_DIR" || { echo "cannot cd into TARGET_DIR $TARGET_DIR"; exit 1; }

    if [ "$1" = "-n" ]
    then

        FEED_END_DATE_INT=$(sqlite3 $SQ_OPTIONS "$WITHDATE_DB" "SELECT feed_end_date FROM feed_info LIMIT 1;" 2> /dev/null)

        PUBLISH_AS_NEWEST="true"
        if [ -n "$FEED_END_DATE_INT" ]
        then
            if [ $(echo $FEED_END_DATE_INT | grep -c -E '^[0-9]{8}$') -eq 1 ]
            then
                TODATE_INT=$(date '+%Y%m%d')
                if [ $FEED_END_DATE_INT -lt $TODATE_INT ]
                then
                    PUBLISH_AS_NEWEST=false
                fi
            fi
        fi

        if [ "$PUBLISH_AS_NEWEST" = 'true' ]
        then
            echo "$(date '+%Y-%m-%d %H:%M:%S') Publish as 'newest'"

            former_newest=$(readlink "$TARGET_SYM")

            # we have a new, valid 'newest' DB, so there's no need to keep older DB with error
            #echo "$(date '+%Y-%m-%d %H:%M:%S') remove symbolic link 'error' (rm -f $ERROR_SYM)"
            rm -f "$ERROR_SYM"

            echo "$(date '+%Y-%m-%d %H:%M:%S') remove symbolic link 'newest' (rm -f $TARGET_SYM)"
            rm -f "$TARGET_SYM"

            echo "$(date '+%Y-%m-%d %H:%M:%S') set symbolic link 'newest' (ln -s $WITHDATE_DB $TARGET_SYM)"
            ln -s "$WITHDATE_DB" "$TARGET_SYM"

            if [ -n "$former_newest" ]
            then
                # now set 'previous' to the former newest
                echo "$(date '+%Y-%m-%d %H:%M:%S') remove symbolic link 'previous' (rm -f $PREVIOUS_SYM)"
                rm -f "$PREVIOUS_SYM"

                echo "$(date '+%Y-%m-%d %H:%M:%S') set symbolic link 'previous' (ln -s $former_newest $PREVIOUS_SYM)"
                ln -s "$former_newest" "$PREVIOUS_SYM"

                RELEASE_DATE=$(sqlite3 $SQ_OPTIONS "$PREVIOUS_SYM" "SELECT release_date FROM ptna WHERE id=1 LIMIT 1;")

                if [ "$use_language" = "de" ]
                then
                    new_comment="Dieses ist eine ältere Version der GTFS-Daten: $RELEASE_DATE"
                else
                    new_comment="This is an older version of the GTFS data: $RELEASE_DATE"
                fi

                echo "$(date '+%Y-%m-%d %H:%M:%S') update comment='$new_comment' for 'previous' $PREVIOUS_SYM"
                sqlite3 $SQ_OPTIONS "$PREVIOUS_SYM" "UPDATE ptna SET comment='$new_comment' WHERE id=1;"
                ret_code=$?
                error_code=$(( $error_code + $ret_code ))
            fi
            cd $DB_DIR

            if [ -f ../post-publish.sh ]
            then
                echo "$(date '+%Y-%m-%d %H:%M:%S') start post publishing $*"
                ../post-publish.sh $*
                ret_code=$?
                error_code=$(( $error_code + $ret_code ))
            fi

        else
            echo "$(date '+%Y-%m-%d %H:%M:%S') 'feed_end_date' of 'feed_info' is in the past. Do not publish as 'newest'"
            error_code=$(( $error_code + 1 ))

            #echo "$(date '+%Y-%m-%d %H:%M:%S') remove symbolic link 'error' (rm -f $ERROR_SYM)"
            rm -f "$ERROR_SYM"

            echo "$(date '+%Y-%m-%d %H:%M:%S') set symbolic link 'error' (ln -s $WITHDATE_DB $ERROR_SYM)"
            ln -s "$WITHDATE_DB" "$ERROR_SYM"

            if [ "$use_language" = "de" ]
            then
                new_comment="Diese Version ist fehlerhaft: das \"feed_end_date\" = \"$FEED_END_DATE_INT\" liegt in der Vergangenheit"
            else
                new_comment="This version has an error: \"feed_end_date\" = \"$FEED_END_DATE_INT\" is in the past"
            fi

            echo "$(date '+%Y-%m-%d %H:%M:%S') update comment='$new_comment' $WITHDATE_DB"
            sqlite3 $SQ_OPTIONS "$WITHDATE_DB" "UPDATE ptna SET comment='$new_comment' WHERE id=1;"
            ret_code=$?
            error_code=$(( $error_code + $ret_code ))
        fi

    elif [ "$1" = "-o" ]
    then
        if [ "$use_language" = "de" ]
        then
            new_comment="Dieses ist eine ältere Version der GTFS-Daten: $RELEASE_DATE"
        else
            new_comment="This is an older version of the GTFS data: $RELEASE_DATE"
        fi

        echo "$(date '+%Y-%m-%d %H:%M:%S') update comment='$new_comment' for this old version $WITHDATE_DB"
        sqlite3 $SQ_OPTIONS "$WITHDATE_DB" "UPDATE ptna SET comment='$new_comment' WHERE id=1;"
        ret_code=$?
        error_code=$(( $error_code + $ret_code ))

    fi

    cd "$GTFS_DIR" || { echo "cannot cd into GTFS_DIR $GTFS_DIR"; exit 1; }
fi

exit $error_code
