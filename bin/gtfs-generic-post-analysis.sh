#!/bin/bash

# this code runs in the folder where the GTFS feed *.zip file has been unpacked

#set -x

error_code=0

# required GTFS files for "genericGtfsImport.py" to work

if [ -f "ptna-gtfs-sqlite.db" ]
then
    if [ -n "$1" ]
    then
        ptna_networks="$*"
    else
        ptna_networks="$(../get-feed-name.sh)"
    fi

    for network in $ptna_networks
    do
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "start post analysis for PTNA network '$network'"

        network_dir=$(find $PTNA_NETWORKS_LOC -type d -name $network)

        if [ -n "$network_dir" ]
        then
            if [ -f "$network_dir/settings.sh" ]
            then
                source "$network_dir/settings.sh"

                if [ -n "$WIKI_ROUTES_PAGE" ]
                then
                    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "processing 'genericGtfsImport.py --database=ptna-gtfs-sqlite.db --gtfs-feed=$network --outfile=$network-catalog.json comment=route_long_name from='trip_headsign|stop_name' to='trip_headsign|stop_name''"

                    genericGtfsImport.py --database     ptna-gtfs-sqlite.db     \
                                         --gtfs-feed    $network                \
                                         --outfile      $network-catalog.json   \
                                         comment=route_long_name                \
                                         from="trip_headsign|stop_name"         \
                                         to="trip_headsign|stop_name"
                    ret_code=$?
                    error_code=$(( $error_code + $ret_code ))

                    if [ $error_code -eq 0 -a -f "./$network-catalog.json" -a -s "./$network-catalog.json" ]
                    then
                        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "reading OSM Wiki page '$WIKI_ROUTES_PAGE'"

                        log=$(ptna-wiki-page.pl --pull --page=$WIKI_ROUTES_PAGE --file=./$network-Wiki-Routes-Page-old.txt 2>&1)
                        ret_code=$?
                        error_code=$(( $error_code + $ret_code ))

                        echo $log | sed -e 's/ \([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] \)/\n\1/g'

                        if [ $error_code -eq 0 -a -f "./$network-Wiki-Routes-Page-old.txt" ]
                        then
                            if [ $(grep -c '#REDIRECT *\[\[' ./$network-Wiki-Routes-Page-old.txt) -eq 0 ]
                            then
                                echo $(date "+%Y-%m-%d %H:%M:%S %Z") "processing 'ptnaFillCsvData.py --routes ./$network-catalog.json --template ./$network-Wiki-Routes-Page-old.txt --outfile ./$network-Wiki-Routes-Page.txt'"

                                ptnaFillCsvData.py --routes   ./$network-catalog.json               \
                                                   --template ./$network-Wiki-Routes-Page-old.txt   \
                                                   --outfile  ./$network-Wiki-Routes-Page.txt
                                ret_code=$?
                                error_code=$(( $error_code + $ret_code ))

                                if [ $ret_code -eq 0 -a -f "./$network-Wiki-Routes-Page.txt" -a -s "./$network-Wiki-Routes-Page.txt" ]
                                then
                                    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "New '$network-Wiki-Routes-Page.txt' has been created"
                                else
                                    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Error: creating new '$network-Wiki-Routes-Page.txt' failed with code: '$ret_code'"
                                fi
                            else
                                error_code=$(( $error_code + 1 ))
                                echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Error: OSM Wiki page '$WIKI_ROUTES_PAGE' includes a '#REDIRECT ...'"
                            fi
                        else
                            echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Error: reading OSM Wiki page '$WIKI_ROUTES_PAGE' failed with code: '$ret_code'"
                        fi
                    else
                        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Error: creating '$network-catalog.json' failed with code: '$ret_code'"
                    fi
                else
                    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Note: '$network' does not have routes data in the OSM Wiki"
                fi
            else
                if [ ! -f "$network_dir/settings.sh" ]
                then
                    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Error: 'settings.sh' file for '$network' not found"
                    error_code=$(( $error_code + 1 ))
                fi
            fi
        fi

        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "done post analysis for PTNA network '$network'"
    done

else
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Error: 'ptna-gtfs-sqlite.db' file not found"
    error_code=$(( $error_code + 1 ))
fi

exit $error_code
