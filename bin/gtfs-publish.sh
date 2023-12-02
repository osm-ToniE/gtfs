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

DB="ptna-gtfs-sqlite.db"

SQ_OPTIONS="-init /dev/null -csv -header"

WORK_BASE_DIR="/osm/ptna/work"

GTFS_DIR=$PWD

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
    COUNTRY=$D2
else
    TARGET_DIR="$WORK_BASE_DIR/$D1/$D2"
    TARGET_SYM="$D1-$D2-$D3-$DB"
    PREVIOUS_SYM="$D1-$D2-$D3-previous-$DB"
    #LONGTERM_SYM="$D1-$D2-$D3-long-term-$DB"
    WITHDATE_DB="$D1-$D2-$D3-$RELEASE_DATE-$DB"
    COUNTRY=$D1
fi

if [ "$COUNTRY" = "DE" ] || [ "$COUNTRY" = "CH" ] || [ "$COUNTRY" = "AT" ]
then
    use_language=de
else
    use_language=en
fi

echo
echo "$(date '+%Y-%m-%d %H:%M:%S') Publish as with 'date' = $RELEASE_DATE"

echo
echo "$(date '+%Y-%m-%d %H:%M:%S') start rsync -tvu $DB $TARGET_DIR/$WITHDATE_DB"
mkdir -p "$TARGET_DIR" 2> /dev/null
rsync -tvu $DB "$TARGET_DIR/$WITHDATE_DB"

cd "$TARGET_DIR" || { echo "cannot cd into TARGET_DIR $TARGET_DIR"; exit 1; }

if [ "$1" = "-n" ]
then

    echo
    echo "$(date '+%Y-%m-%d %H:%M:%S') Publish as 'newest'"

    former_newest=$(readlink "$TARGET_SYM")

    echo
    echo "$(date '+%Y-%m-%d %H:%M:%S') remove symbolic link 'newest' (rm -f $TARGET_SYM)"
    rm -f "$TARGET_SYM"

    echo
    echo "$(date '+%Y-%m-%d %H:%M:%S') set symbolic link 'newest' (ln -s $WITHDATE_DB $TARGET_SYM)"
    ln -s "$WITHDATE_DB" "$TARGET_SYM"

    if [ -n "$former_newest" ]
    then
        echo
        echo "$(date '+%Y-%m-%d %H:%M:%S') remove symbolic link 'previous' (rm -f $PREVIOUS_SYM)"
        rm -f "$PREVIOUS_SYM"

        echo
        echo "$(date '+%Y-%m-%d %H:%M:%S') set symbolic link 'previous' (ln -s $former_newest $PREVIOUS_SYM)"
        ln -s "$former_newest" "$PREVIOUS_SYM"

        RELEASE_DATE=$(sqlite3 $SQ_OPTIONS "$PREVIOUS_SYM" "SELECT release_date FROM ptna WHERE id=1 LIMIT 1;")

        if [ "$use_language" = "de" ]
        then
            new_comment="Dieses ist eine ältere Version der GTFS-Daten: $RELEASE_DATE"
        else
            new_comment="This is an older version of the GTFS data: $RELEASE_DATE"
        fi

        echo
        echo "$(date '+%Y-%m-%d %H:%M:%S') update comment='$new_comment' for 'previous' $PREVIOUS_SYM"
        sqlite3 $SQ_OPTIONS "$PREVIOUS_SYM" "UPDATE ptna SET comment='$new_comment' WHERE id=1;"
    fi

elif [ "$1" = "-o" ]
then
    if [ "$use_language" = "de" ]
    then
        new_comment="Dieses ist eine ältere Version der GTFS-Daten: $RELEASE_DATE"
    else
        new_comment="This is an older version of the GTFS data: $RELEASE_DATE"
    fi

    echo
    echo "$(date '+%Y-%m-%d %H:%M:%S') update comment='$new_comment' for this old version $WITHDATE_DB"
    sqlite3 $SQ_OPTIONS "$WITHDATE_DB" "UPDATE ptna SET comment='$new_comment' WHERE id=1;"

fi

cd "$GTFS_DIR" || { echo "cannot cd into GTFS_DIR $GTFS_DIR"; exit 1; }
