#!/bin/bash

DB="ptna-gtfs-sqlite.db"

SQ_OPTIONS="-init /dev/null -csv -header"

rm -f $DB

#rm -f agency.txt calendar.txt calendar_dates.txt fare_attributes.txt fare_rules.txt feed_info.txt frequencies.txt routes.txt trips.txt stops.txt stop_times.txt shapes.txt transfers.txt

#
# unzip the GTFS file, overwriting existing ones
#

unzip -o -- *.zip

today=$(date '+%Y-%m-%d')

# $PWD should include the date of the release like: "/osm/ptna/work/gtfs-networks/DE/BY/MVV/2020-03-17"
# otherwise, the date is taken from the "time of last data modification" from some files of the zip
# if everything fails, the current day will be taken

release_date=$(basename "$PWD")

if [ "$(echo "$release_date" | grep -E -c '[0-9]{4}-[01][0-9]-[0123][0-9]')" = 0 ]
then
    if [ -f feed_info.txt ]
    then
        release_date=$(date --date="$(stat --format=%y feed_info.txt)" '+%Y-%m-%d')
    elif [ -f agency.txt ]
    then
        release_date=$(date --date="$(stat --format=%y agency.txt)" '+%Y-%m-%d')
    elif [ -f routes.txt ]
    then
        release_date=$(date --date="$(stat --format=%y routes.txt)" '+%Y-%m-%d')
    else
        release_date=$(date '+%Y-%m-%d')
    fi
fi



#
# create a TABLE with OSM specific information for route relations: 'network', 'network:short', 'network:guid', gtfs_agency_is_operator (true/false), 'trip_id_regex'
#

echo "Table 'osm'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS osm;"
if [ -f ../osm.txt ]
then
    sqlite3 $SQ_OPTIONS "$DB" ".import ../osm.txt osm"
    sqlite3 $SQ_OPTIONS "$DB" "UPDATE osm SET prepared='$today' WHERE id=1;"
else
    columns="id INTEGER DEFAULT 1 PRIMARY KEY, prepared TEXT DEFAULT '', network TEXT DEFAULT '', network_short TEXT DEFAULT '', network_guid TEXT DEFAULT '', gtfs_agency_is_operator INTEGER DEFAULT 0, trip_id_regex TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE osm ($columns);"
    sqlite3 $SQ_OPTIONS "$DB" "INSERT INTO osm (id,prepared) VALUES (1,'$today');"
    sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM osm;" > ../osm.txt
fi


#
# create a TABLE with PTNA specific information: license, release date, modification date, ...
#

echo "Table 'ptna'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS ptna;"
if [ -f ../ptna.txt ]
then
    sqlite3 $SQ_OPTIONS "$DB" ".import ../ptna.txt ptna"
    sqlite3 $SQ_OPTIONS "$DB" "UPDATE ptna SET prepared='$today', aggregated='', analyzed='', normalized='', release_date='$release_date' WHERE id=1;"
else
    columns="id INTEGER DEFAULT 1 PRIMARY KEY, network_name TEXT DEFAULT '', network_name_url TEXT DEFAULT '', prepared TEXT DEFAULT '', aggregated TEXT DEFAULT '', analyzed TEXT DEFAULT '', normalized TEXT DEFAULT '', feed_publisher_name TEXT DEFAULT '',feed_publisher_url TEXT DEFAULT '', release_date TEXT DEFAULT '', release_url TEXT DEFAULT '', license TEXT DEFAULT '', license_url TEXT DEFAULT '', original_license TEXT DEFAULT '', original_license_url TEXT DEFAULT '', has_shapes INTEGER DEFAULT 0, consider_calendar INTEGER DEFAULT 0, comment TEXT DEFAULT '', details TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna ($columns);"
    sqlite3 $SQ_OPTIONS "$DB" "INSERT INTO ptna (id,prepared,release_date) VALUES (1,'$today','$release_date');"
    sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM ptna;" > ../ptna.txt
fi

#
# will store normalized routes information summary of what has been changed on table "routes"
#

echo "Table 'ptna_routes'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS ptna_routes;"
sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_routes (route_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, normalized_route_long_name TEXT DEFAULT '');"


#
# will store aggregated trip information summary of what has been removed from table "trips"
#

echo "Table 'ptna_trips'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS ptna_trips;"
sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_trips (trip_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, list_trip_ids TEXT DEFAULT '', list_departure_times TEXT DEFAULT '', list_durations TEXT DEFAULT '', list_service_ids TEXT DEFAULT '', min_start_date TEXT DEFAULT '', max_end_date TEXT DEFAULT '', rides INTEGER DEFAULT 0, sum_rides INTEGER DEFAULT 0, route_id TEXT DEFAULT '');"


#
# will store normalized stops information summary of what has been changed on table "stops"
#

echo "Table 'ptna_stops'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS ptna_stops;"
sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_stops (stop_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, normalized_stop_name TEXT DEFAULT '');"


#
# will store comments on routes
#

echo "Table 'ptna_routes_comments'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS ptna_routes_comments;"
sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_routes_comments (route_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, comment TEXT DEFAULT '');"


#
# will store comments on trips
#

echo "Table 'ptna_trips_comments'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS ptna_trips_comments;"
sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_trips_comments (trip_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, comment TEXT DEFAULT '', subroute_of TEXT DEFAULT '', suspicious_start TEXT DEFAULT '', suspicious_end TEXT DEFAULT '', suspicious_number_of_stops TEXT DEFAULT '', same_names_but_different_ids TEXT DEFAULT '', suspicious_trip_duration TEXT DEFAULT '', same_stops_but_different_shape_ids TEXT DEFAULT '', suspicious_other TEXT DEFAULT '' );"


#
# will store aggregation results
#

echo "Table 'ptna_aggregation'"

sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_aggregation ('id' INTEGER DEFAULT 0 PRIMARY KEY, 'date' TEXT DEFAULT '', 'duration' INTEGER DEFAULT 0);"


#
# will store analysis results
#

echo "Table 'ptna_analysis'"

sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_analysis ('id' INTEGER DEFAULT 0 PRIMARY KEY, 'date' TEXT DEFAULT '', 'duration' INTEGER DEFAULT 0);"


#
# will store normaization results
#

echo "Table 'ptna_normalization'"

sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE ptna_normalization ('id' INTEGER DEFAULT 0 PRIMARY KEY, 'date' TEXT DEFAULT '', 'duration' INTEGER DEFAULT 0);"


#
# agency.txt - we take it as it is, no PRIMARY KEY defined, ...
#

echo "Table 'agency'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS agency;"
if [ -f agency.txt ]
then
    columns=$(head -1 agency.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/agency_id TEXT/agency_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
    if [ "$(head -1 agency.txt | grep -F -c agency_id)" == 1 ]
    then
        grep -F -v agency_id agency.txt | grep -E -v '^\s*$' > agency-wo-header.txt
    else
        columns="agency_id TEXT PRIMARY KEY,$columns"
        grep -F -v agency_name agency.txt | grep  -E -v '^\s*$' | awk '{printf("%d,%s\n", NR, $0)}' > agency-wo-header.txt
    fi
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE agency ($columns);"
    AGENCY_COUNT=$(cat agency-wo-header.txt | wc -l)
    if [ "$AGENCY_COUNT" -gt 0 ]
    then
        sqlite3 $SQ_OPTIONS "$DB" ".import agency-wo-header.txt agency"
    else
        sqlite3 $SQ_OPTIONS "$DB" "INSERT INTO agency (agency_id,agency_name) VALUES ('1','???');"
        AGENCY_COUNT=1
    fi
    rm -f agency-wo-header.txt
fi


#
# calendar_dates.txt - we take it as it is, no PRIMARY KEY defined, ... but create index on service_id
#

echo "Table 'calendar_dates'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS calendar_dates;"
if [ -f calendar_dates.txt -a -s calendar_dates.txt ]
then
    if [ $(head -1 calendar_dates.txt | grep -F -c service_id) -gt 0 ]
    then
        grep -F -v service_id calendar_dates.txt | grep -E -v '^\s*$' > calendar_dates-wo-header.txt
        if [ "$(stat -c%s calendar_dates-wo-header.txt)" -gt 0 ]
        then
            columns=$(head -1 calendar_dates.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/$/ TEXT/g' -e 's/,/ TEXT, /g' -e 's/[\r\n]//gi')
            sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE calendar_dates ($columns);"
            sqlite3 $SQ_OPTIONS "$DB" ".import calendar_dates-wo-header.txt calendar_dates"
            rm -f calendar_dates-wo-header.txt
        else
            columns="service_id TEXT, date TEXT DEFAULT '', exception_type INTEGER DEFAULT 0, ptna_changedate TEXT DEFAULT ''"
            sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE calendar_dates ($columns);"
        fi
    else
        echo "calendar_dats.txt without header"
        rm -f "$DB"
        exit 1
    fi
else
    columns="service_id TEXT, date TEXT DEFAULT '', exception_type INTEGER DEFAULT 0, ptna_changedate TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE calendar_dates ($columns);"
fi
sqlite3 $SQ_OPTIONS "$DB" "CREATE INDEX idx_service_id ON calendar_dates (service_id);"


#
# calendar.txt - service_id is PRIMARY KEY, ...
#

echo "Table 'calendar'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS calendar;"
rm -f calendar-wo-header.txt
touch calendar-wo-header.txt
if [ -f calendar.txt -a -s calendar.txt ]
then
    if [ $(head -1 calendar.txt | grep -F -c service_id) -gt 0 ]
    then
        columns=$(head -1 calendar.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/service_id TEXT/service_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
        grep -F -v service_id calendar.txt | grep -E -v '^\s*$' > calendar-wo-header.txt
        if [ "$(stat -c%s calendar-wo-header.txt)" -gt 0 ]
        then
            sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE calendar ($columns);"
            sqlite3 $SQ_OPTIONS "$DB" ".import calendar-wo-header.txt calendar"
        fi
    else
        echo "calendar.txt without header"
        rm -f "$DB"
        exit 1
    fi
fi
if [ "$(stat -c%s calendar-wo-header.txt)" -eq 0 ]
then
    columns="service_id TEXT PRIMARY KEY, monday INTEGER DEFAULT 0, tuesday INTEGER DEFAULT 0, wednesday INTEGER DEFAULT 0, thursday INTEGER DEFAULT 0, friday INTEGER DEFAULT 0, saturday INTEGER DEFAULT 0, sunday INTEGER DEFAULT 0, start_date TEXT DEFAULT '', end_date TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE calendar ($columns);"
    sqlite3 $SQ_OPTIONS "$DB" "INSERT INTO calendar (service_id) SELECT DISTINCT service_id FROM calendar_dates;"
    sqlite3 $SQ_OPTIONS "$DB" "UPDATE calendar SET start_date = (SELECT date FROM calendar_dates ORDER BY CAST (date AS INTEGER) ASC  LIMIT 1);"
    sqlite3 $SQ_OPTIONS "$DB" "UPDATE calendar SET end_date   = (SELECT date FROM calendar_dates ORDER BY CAST (date AS INTEGER) DESC LIMIT 1);"
    sqlite3 $SQ_OPTIONS "$DB" "SELECT * from calendar;"
fi
rm -f calendar-wo-header.txt


#
# feed_info.txt - we take it as it is, no PRIMARY KEY defined, ...
#

echo "Table 'feed_info'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS feed_info;"
if [ -f feed_info.txt ]
then
    sqlite3 $SQ_OPTIONS "$DB" ".import feed_info.txt feed_info"
else
    columns="feed_publisher_name TEXT DEFAULT '',feed_publisher_url TEXT DEFAULT '',feed_lang TEXT DEFAULT '',feed_start_date TEXT DEFAULT '',feed_end_date TEXT DEFAULT '',feed_version TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE feed_info ($columns);"
fi


#
# routes.txt - route_id is PRIMARY KEY, ...
#

echo "Table 'routes'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS routes;"
if [ -f routes.txt ]
then
    if [ $(head -1 routes.txt | grep -F -c route_id) -gt 0 ]
    then
        columns=$(head -1 routes.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/route_id TEXT/route_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
        grep -F -v route_id routes.txt | grep -E -v '^\s*$' > routes-wo-header.txt
        sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE routes ($columns);"
        sqlite3 $SQ_OPTIONS "$DB" ".import routes-wo-header.txt routes"
        if [ "$(head -1 routes.txt | grep -F -c agency_id)" == 0 ]
        then
            sqlite3 $SQ_OPTIONS "$DB" "ALTER TABLE routes ADD agency_id TEXT DEFAULT '';"
        fi
        if [ "$(head -1 routes.txt | grep -F -c route_long_name)" == 0 ]
        then
            sqlite3 $SQ_OPTIONS "$DB" "ALTER TABLE routes ADD route_long_name TEXT DEFAULT '?';"
        fi
        if [ "$AGENCY_COUNT" -eq 1 ]
        then
            sqlite3 $SQ_OPTIONS "$DB" "UPDATE routes SET agency_id=(SELECT agency_id from agency) WHERE agency_id='';"
        fi
        sqlite3 $SQ_OPTIONS "$DB" "UPDATE routes SET route_short_name = route_long_name WHERE route_short_name='';"
        rm -f routes-wo-header.txt
    else
        echo "routes.txt without header"
        rm -f "$DB"
        exit 1
    fi
fi


#
# shapes.txt - no PRIMARY KEY, ... but create index over shape_id
#

echo "Table 'shapes'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS shapes;"
if [ -f shapes.txt -a -s shapes.txt ]
then
    if [ $(head -1 shapes.txt | grep -F -c shape_id) -gt 0 ]
    then
        columns=$(head -1 shapes.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/[\r\n]//gi')
        grep -F -v shape_id shapes.txt | grep -E -v '^\s*$' > shapes-wo-header.txt
        sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE shapes ($columns);"
        sqlite3 $SQ_OPTIONS "$DB" ".import shapes-wo-header.txt shapes"
        rm -f shapes-wo-header.txt
    else
        echo "shapes.txt without header"
        rm -f "$DB"
        exit 1
    fi
else
    columns="shape_id TEXT,shape_pt_lat TEXT DEFAULT '',shape_pt_lon TEXT DEFAULT '',shape_pt_sequence TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE shapes ($columns);"
fi
sqlite3 $SQ_OPTIONS "$DB" "CREATE INDEX idx_shape_id ON shapes (shape_id);"
sqlite3 $SQ_OPTIONS "$DB" "UPDATE ptna SET has_shapes=(SELECT COUNT(*) FROM shapes);"


#
# stops.txt - stop_id is PRIMARY KEY, ...
#

echo "Table 'stops'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS stops;"
if [ -f stops.txt ]
then
    if [ $(head -1 stops.txt | grep -F -c stop_id) -gt 0 ]
    then
        columns=$(head -1 stops.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/stop_id TEXT/stop_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
        grep -F -v stop_id stops.txt | grep -E -v '^\s*$' > stops-wo-header.txt
        sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE stops ($columns);"
        sqlite3 $SQ_OPTIONS "$DB" ".import stops-wo-header.txt stops"
        rm -f stops-wo-header.txt
    else
        echo "stops.txt without header"
        rm -f "$DB"
        exit 1
    fi
fi


#
# stop_times.txt - we take it as it is, no PRIMARY KEY defined, ... but create index on trip_id adn stop_id
#

echo "Table 'stop_times'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS stop_times;"
if [ -f stop_times.txt ]
then
    if [ $(head -1 stop_times.txt | grep -F -c stop_id) -gt 0 ]
    then
        columns=$(head -1 stop_times.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/[\r\n]//gi')
        grep -F -v stop_id stop_times.txt | grep -E -v '^\s*$' > stop_times-wo-header.txt
        sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE stop_times ($columns);"
        sqlite3 $SQ_OPTIONS "$DB" ".import stop_times.txt stop_times"
        sqlite3 $SQ_OPTIONS "$DB" "CREATE INDEX idx_trip_id ON stop_times (trip_id);"
        sqlite3 $SQ_OPTIONS "$DB" "CREATE INDEX idx_stop_id ON stop_times (stop_id);"
        rm -f stop_times-wo-header.txt
    else
        echo "stop_times.txt without header"
        rm -f "$DB"
        exit 1
    fi
fi


#
# trips.txt - trip_id is PRIMARY KEY, ... create also index on route_id
#

echo "Table 'trips'"

sqlite3 $SQ_OPTIONS "$DB" "DROP TABLE IF EXISTS trips;"
if [ -f trips.txt ]
then
    if [ $(head -1 trips.txt | grep -F -c trip_id) -gt 0 ]
    then
        columns=$(head -1 trips.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/$/ TEXT/g' -e 's/trip_id TEXT/trip_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
        grep -F -v trip_id trips.txt | grep -E -v '^\s*$' > trips-wo-header.txt
        sqlite3 $SQ_OPTIONS "$DB" "CREATE TABLE trips ($columns);"
        sqlite3 $SQ_OPTIONS "$DB" ".import trips-wo-header.txt trips"
        sqlite3 $SQ_OPTIONS "$DB" "CREATE INDEX idx_route_id ON trips (route_id);"
        rm -f trips-wo-header.txt
    else
        echo "trips.txt without header"
        rm -f "$DB"
        exit 1
    fi
fi

sqlite3 $SQ_OPTIONS "$DB" ".schema"

echo
echo "Test for agency_id from agency"
sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM agency LIMIT 2;"

echo
echo "Test for route_id from routes"
sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM routes LIMIT 1;"

echo
echo "Test for trip_id from trips"
sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM trips LIMIT 1;"

echo
echo "Test for route_id and trip_id from trips"
sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM trips LIMIT 1;"

echo
echo "OSM settings"
sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM osm;"
echo
echo "PTNA settings"
sqlite3 $SQ_OPTIONS "$DB" "SELECT * FROM ptna;"
