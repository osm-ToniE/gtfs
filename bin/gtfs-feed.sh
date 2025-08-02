#!/bin/bash

#
# Analysis for a single GTFS feed
#
# execute this in the directory of the feed definition
# expected files: get_release_date.sh, get_release_url.sh, cleanup.sh
#

WORK_BASE_DIR="/osm/ptna/work"

TEMP=$(getopt -o acdDEfnoPTuvW: --long analyze,clean,date-print,date-check,clean-empty,feed-print,new,old,publish,touch-non-existent,url-print,verbose,wipe-old -n 'gtfs-feed.sh' -- "$@")

# shellcheck disable=SC2181
if [ $? != 0 ] ; then echo "$(date '+%Y-%m-%d %H:%M:%S') Terminating..."  1>&2 ; exit 2 ; fi

eval set -- "$TEMP"

while true ; do
    case "$1" in
        -a|--analyze)               analyze=true                        ; shift ;;
        -c|--clean)                 clean=true                          ; shift ;;
        -d|--date-print)            date_print=true                     ; shift ;;
        -D|--date-check)            date_check=true                     ; shift ;;
        -E|--clean-empty)           clean_empty=true                    ; shift ;;
        -f|--feed)                  feed_print=true                     ; shift ;;
        -n|--new)                   publish_as_new='-n'                 ; shift ;;
        -o|--old)                   publish_as_old='-o'                 ; shift ;;
        -P|--publish)               publish=true                        ; shift ;;
        -T|--touch-non-existent)    touch_n_e=true                      ; shift ;;
        -u|--url-print)             url_print=true                      ; shift ;;
        -v|--verbose)               verbose='-v'                        ; shift ;;
        -W|--wipe-old)              wipe_old=true       ; keep_file=$2  ; shift 2;;
        --) shift ; break ;;
        *) echo "$(date '+%Y-%m-%d %H:%M:%S') Internal error!" 1>&2 ; exit 3 ;;
    esac
done

error_code=0

#
#
#

if [ "$clean" = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Removing temporary files" 1>&2
    if [ -f ./cleanup.sh ]
    then
        ./cleanup.sh "$verbose"
    fi
    rm -rf 20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]*
fi

#
#
#

if [ "$date_print" = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Retrieving Release-Date" 1>&2
    if [ -f ./get-release-date.sh ]
    then
        ./get-release-date.sh "$verbose"
    else
        echo manually 1>&2
    fi
fi

#
#
#

if [ "$date_check" = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Checking Release-Date against existing feeds" 1>&2
    if [ -f ./get-release-date.sh ] && [ -f ./get-feed-name.sh ]
    then
        FEED_NAME=$(./get-feed-name.sh)

        if [ -n "$FEED_NAME" ]
        then
            printf "%-32s - " "$FEED_NAME"
            RELEASE_DATE=$(./get-release-date.sh)

            if [[ "$RELEASE_DATE" =~ ^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$ ]]
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

                if [ -f "$PTNA_WORK_LOC/$COUNTRY_DIR/$FEED_NAME-ptna-gtfs-sqlite.db" ]
                then
                    WORK_LOC="$PTNA_WORK_LOC/$COUNTRY_DIR"
                elif [ -f "$PTNA_WORK_LOC/$SUB_DIR/$FEED_NAME-ptna-gtfs-sqlite.db" ]
                then
                    WORK_LOC="$PTNA_WORK_LOC/$SUB_DIR"
                else
                    WORK_LOC="$PTNA_WORK_LOC/$SUB_DIR"
                fi
                mkdir -p "$WORK_LOC" 2> /dev/null

                if [ -f "$WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db" ]
                then
                    if [ -s "$WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db" ]
                    then
                        printf "%s - OK\n" "$RELEASE_DATE"
                    else
                        youngest_real=$(find "$WORK_LOC/" -type f -size +1 -name "$FEED_NAME-20*-ptna-gtfs-sqlite.db" | sort | tail -1 | sed -e "s/^.*$FEED_NAME-//" -e 's/-ptna-gtfs-sqlite.db$//')
                        youngest_real_Ym=$(echo "$youngest_real" | cut -c 1-7 | sed -e 's/-//')
                        RELEASE_DATE_Ym=$(echo  "$RELEASE_DATE"  | cut -c 1-7 | sed -e 's/-//')
                        if [ -n "$youngest_real_Ym" ]
                        then
                            if [ "$youngest_real_Ym" -eq "$RELEASE_DATE_Ym" ]
                            then
                                printf "%s versus %s - skip(ped) version\n" "$youngest_real" "$RELEASE_DATE"
                            elif [ "$youngest_real_Ym" -gt "$RELEASE_DATE_Ym" ]
                            then
                                printf "%s versus %s - older release date?\n" "$youngest_real" "$RELEASE_DATE"
                            else
                                printf "%s versus %s - not yet analyzed (stub)\n" "$youngest_real" "$RELEASE_DATE"
                            fi
                        else
                            printf "%s is new - not yet analyzed (stub)\n" "$RELEASE_DATE"
                        fi
                    fi
                else
                    youngest_real=$(find "$WORK_LOC/" -type f -size +1 -name "$FEED_NAME-20*-ptna-gtfs-sqlite.db" | sort | tail -1 | sed -e "s/^.*$FEED_NAME-//" -e 's/-ptna-gtfs-sqlite.db$//')

                    youngest_real_Ym=$(echo "$youngest_real" | cut -c 1-7 | sed -e 's/-//')
                    RELEASE_DATE_Ym=$( echo "$RELEASE_DATE"  | cut -c 1-7 | sed -e 's/-//')
                    if [ -n "$youngest_real_Ym" ]
                    then
                        if [ "$youngest_real_Ym" -eq "$RELEASE_DATE_Ym" ]
                        then
                            printf "%s versus %s - same month\n" "$youngest_real" "$RELEASE_DATE"
                            if [ "$touch_n_e" = "true" ]
                            then
                                touch "$WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db"
                            fi
                        elif [ "$youngest_real_Ym" -gt "$RELEASE_DATE_Ym" ]
                        then
                            printf "%s versus %s - older release date?\n" "$youngest_real" "$RELEASE_DATE"
                        else
                            printf "%s versus %s - not yet analyzed (new)\n" "$youngest_real" "$RELEASE_DATE"
                            if [ "$touch_n_e" = "true" ]
                            then
                                touch "$WORK_LOC/$FEED_NAME-$RELEASE_DATE-ptna-gtfs-sqlite.db"
                            fi
                        fi
                    else
                        printf "%s is new - not yet analyzed (new)\n" "$RELEASE_DATE"
                    fi
                fi
            else
                if [ -n "$RELEASE_DATE" ]
                then
                    printf "unknown release date: '$RELEASE_DATE'\n"
                elif [ -f ./release_date_error.log -a -s ./release_date_error.log ]
                then
                    printf "unknown release date: "
                    cat ./release_date_error.log
                    rm -f ./release_date_error.log
                else
                    printf "unknown release date\n"
                fi
            fi
        else
            printf "%-32s - feed name is null\n" "$PWD"
        fi
    else
        FEED_NAME=$(echo "$PWD" | sed -e "s|^$GTFS_FEEDS_LOC/||" -e 's|/|-|g')
        printf "%-32s - manually\n" "$FEED_NAME" 1>&2
    fi
fi

#
#
#

if [ "$feed_print" = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Retrieving Feed Name" 1>&2
    if [ -f ./get-feed-name.sh ]
    then
        ./get-feed-name.sh "$verbose"
    else
        echo manually 1>&2
    fi
fi

#
#
#

if [ "$url_print"  = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Retrieving Release-URL" 1>&2
    if [ -f ./get-release-url.sh ]
    then
        ./get-release-url.sh "$verbose"
    else
        echo manually 1>&2
    fi
fi

#
#
#

if [ "$analyze"  = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Analyzing GTFS package" 1>&2
    if [ -f ./get-release-url.sh ] && [ -f ./get-release-date.sh ]
    then
        rd=$(./get-release-date.sh)
        ru=$(./get-release-url.sh)
        if [ -n "$rd" ] && [ -n "$ru" ]
        then
            [ -d "$rd" ] || mkdir "$rd"
            if [ -f ./get-release-file.sh ]
            then
                ./get-release-file.sh > "$rd/gtfs.zip"
            else
                echo "$(date '+%Y-%m-%d %H:%M:%S') Download GTFS data from '$ru'"
                wget --no-verbose --user-agent "PTNA script on https://ptna.openstreetmap.de" -O "$rd/gtfs.zip" "$ru"
            fi
            if [ -f "$rd/gtfs.zip" ] && [ -s "$rd/gtfs.zip" ]
            then
                if [ "$(zipinfo -s "$rd/gtfs.zip" | grep -E -c -i 'Zip file size')" == 1 ]
                then
                    (cd "$rd" && gtfs-handle-zip.sh $verbose)
                    ret_code=$?
                    error_code=$(( $error_code + $ret_code ))

                else
                    echo "failed (file not Zip)"  1>&2
                    error_code=$(( $error_code + 1 ))
                fi
            else
                echo "failed (file is empty)" 1>&2
                error_code=$(( $error_code + 1 ))
            fi
        else
            echo failed 1>&2
            error_code=$(( $error_code + 1 ))
        fi
    else
        echo manually 1>&2
        error_code=$(( $error_code + 1 ))
    fi
fi

#
#
#

if [ "$publish"  = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Publishing data" 1>&2
    db=$(find . -maxdepth 2 -mindepth 1 -name ptna-gtfs-sqlite.db | sort | tail -1)
    if [ -n "$db" ]
    then
        rd=$(dirname "$db")
        if [ -n "$rd" ] && [ -d "$rd" ]
        then
            (cd "$rd" && gtfs-publish.sh "$publish_as_new" "$publish_as_old")
        else
            echo "failed for directory $rd" 1>&2
        fi
    else
        echo "failed: 'ptna-gtfs-sqlite.db' not found" 1>&2
        error_code=$(( $error_code + 1 ))
    fi
fi


if [ "$clean_empty"  = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Clean up older empty databases" 1>&2

    feed=$(./get-feed-name.sh)

    current=$(find $WORK_BASE_DIR -name "${feed}-ptna-gtfs-sqlite.db")

    if [ -n "$current" ]
    then
        find $WORK_BASE_DIR -name "${feed}-20*ptna-gtfs-sqlite.db" -size 0c ! -newer "$current" -exec rm {} \;
    fi
fi


if [ "$wipe_old"  = "true" ]
then
    [ -n "$verbose" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') Wipe out older, not referenced databases (not yet realized)" 1>&2

    feed=$(./get-feed-name.sh)

    if [ -n "$keep_file" -a -f "$keep_file" ]
    then
        # delete all $feed-%Y-%m-%d-ptna-gtfs-sqlite.db files except those referenced by
        # - $feed-ptna-gtfs-sqlite.db           as a symbolic link
        # - $feed-previous-ptna-gtfs-sqlite.db  as a symbolic link
        # - $feed-long-term-ptna-gtfs-sqlite.db as a symbilic link
        # - listed in the keep-file

        existing_files_with_date=$(find $WORK_BASE_DIR -name "${feed}-20*ptna-gtfs-sqlite.db" -size +0c -printf "%p " | sort -nr)
        current_points_to=$(find $WORK_BASE_DIR -name "${feed}-ptna-gtfs-sqlite.db" -exec readlink -f {} \;)
        previous_points_to=$(find $WORK_BASE_DIR -name "${feed}-previous-ptna-gtfs-sqlite.db" -exec readlink -f {} \;)
        long_term_points_to=$(find $WORK_BASE_DIR -name "${feed}-long-termptna-gtfs-sqlite.db" -exec readlink -f {} \;)

        echo "Existing Files : $existing_files_with_date"
        echo "Long-Term File : $long_term_points_to"
        echo "Previous File  : $previous_points_to"
        echo "Current File   : $current_points_to"

        for existing_file in $existing_files_with_date
        do
            echo "Check File : $existing_file"
            if [ -n "$long_term_points_to" -a "$long_term_points_to" == "$existing_file" ]
            then
                echo "    Keep As Long-Term File"
                continue
            fi
            if [ -n "$previous_points_to" -a "$previous_points_to" == "$existing_file" ]
            then
                echo "    Keep As Previous File"
                continue
            fi
            if [ -n "$current_points_to" -a "$current_points_to" == "$existing_file" ]
            then
                echo "    Keep As Current File"
                continue
            fi
            short_name=$(basename $existing_file -ptna-gtfs-sqlite.db)
            echo "Check Short Name : '$short_name' Against Keep File '$keep_file'"
            if [ $(grep -F -c "$short_name" "$keep_file") -gt 0 ]
            then
                echo "    To Be Kept : $existing_file"
                continue
            fi
            echo "    Delete File : $existing_file ?"
        done
    else
        if [ -z "$keep_file" ]
        then
            echo "Keep File Not Specified For Option '-W ...'"
        else
            echo "Keep File For Option '-W $keep_file' does not exist"
        fi
    fi

fi

exit $error_code
