#!/bin/bash

DB="ptna-gtfs-sqlite.db"

SQ_OPTIONS="-csv -header"

rm -f $DB

rm -f agency.txt calendar.txt calendar_dates.txt feed_info.txt routes.txt trips.txt stops.txt stop_times.txt shapes.txt transfers.txt

#
# unzip the GTFS file, overwriting existing ones
#

unzip -o *.zip

today=$(date '+%Y-%m-%d')

# $PWD should include the date of the release like: "/osm/ptna/work/gtfs-networks/DE/BY/MVV/2020-03-17"
# otherwise, the date is taken from the "time of last data modification" from some files of the zip
# if everything fails, the current day will be taken

release_date=$(basename $PWD)

if [ $(echo $release_date | egrep -c '[0-9]{4}-[01][0-9]-[0123][0-9]') = 0 ]
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

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS osm;"
if [ -f ../osm.txt ]
then
    sqlite3 $SQ_OPTIONS $DB ".import ../osm.txt osm"
    sqlite3 $SQ_OPTIONS $DB "UPDATE osm SET prepared='$today' WHERE id=1;"
else
    columns="id INTEGER DEFAULT 1 PRIMARY KEY, prepared TEXT DEFAULT '', network TEXT DEFAULT '', network_short TEXT DEFAULT '', network_guid TEXT DEFAULT '', gtfs_agency_is_operator INTEGER DEFAULT 0, trip_id_regex TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE osm ($columns);"
    sqlite3 $SQ_OPTIONS $DB "INSERT INTO osm (id,prepared) VALUES (1,'$today');"
    sqlite3 $SQ_OPTIONS $DB "SELECT * FROM osm;" > ../osm.txt
fi


#
# create a TABLE with PTNA specific information: license, release date, modification date, ...
#

echo "Table 'ptna'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS ptna;"
if [ -f ../ptna.txt ]
then
    sqlite3 $SQ_OPTIONS $DB ".import ../ptna.txt ptna"
    sqlite3 $SQ_OPTIONS $DB "UPDATE ptna SET prepared='$today', aggregated='', analyzed='', normalized='', release_date='$release_date' WHERE id=1;"
else
    columns="id INTEGER DEFAULT 1 PRIMARY KEY, network_name TEXT DEFAULT '', network_name_url TEXT DEFAULT '', prepared TEXT DEFAULT '', aggregated TEXT DEFAULT '', analyzed TEXT DEFAULT '', normalized TEXT DEFAULT '', feed_publisher_name TEXT DEFAULT '',feed_publisher_url TEXT DEFAULT '', release_date TEXT DEFAULT '', release_url TEXT DEFAULT '', license TEXT DEFAULT '', license_url TEXT DEFAULT '', original_license TEXT DEFAULT '', original_license_url TEXT DEFAULT '', has_shapes INTEGER DEFAULT 0, ignore_calendar INTEGER DEFAULT 0, comment TEXT DEFAULT '', details TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE ptna ($columns);"
    sqlite3 $SQ_OPTIONS $DB "INSERT INTO ptna (id,prepared,release_date) VALUES (1,'$today','$release_date');"
    sqlite3 $SQ_OPTIONS $DB "SELECT * FROM ptna;" > ../ptna.txt
fi


echo "Table 'ptna_trips'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS ptna_trips;"
sqlite3 $SQ_OPTIONS $DB "CREATE TABLE ptna_trips (trip_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, list_trip_ids TEXT DEFAULT '', list_departure_times TEXT DEFAULT '');"


#
# agency.txt - we take it as it is, no PRIMARY KEY defined, ...
#

echo "Table 'agency'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS agency;"
if [ -f agency.txt ]
then
    columns=$(head -1 agency.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/agency_id TEXT/agency_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
    fgrep -v agency_id agency.txt > agency-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE agency ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import agency-wo-header.txt agency"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE agency ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE agency ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE agency ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE agency ADD ptna_comment    TEXT DEFAULT '';"
    rm -f agency-wo-header.txt
fi


#
# calendar_dates.txt - we take it as it is, no PRIMARY KEY defined, ...
#

echo "Table 'calendar_dates'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS calendar_dates;"
if [ -f calendar_dates.txt ]
then
    columns=$(head -1 calendar_dates.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/[\r\n]//gi')
    fgrep -v service_id calendar_dates.txt > calendar_dates-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE calendar_dates ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import calendar_dates-wo-header.txt calendar_dates"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE calendar_dates ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE calendar_dates ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE calendar_dates ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE calendar_dates ADD ptna_comment    TEXT DEFAULT '';"
#    sqlite3 $SQ_OPTIONS $DB "CREATE INDEX idx_service_id ON  calendar_dates (service_id);"
    rm -f calendar_dates-wo-header.txt
else
    columns="service_id TEXT PRIMARY KEY,date TEXT DEFAULT '',exception_type INTEGER DEFAULT 0, ptna_changedate TEXT DEFAULT '', ptna_is_invalid TEXT DEFAULT '', ptna_is_wrong TEXT DEFAULT '', ptna_comment TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE calendar_dates ($columns);"
fi


#
# calendar.txt - service_id is PRIMARY KEY, ...
#

echo "Table 'calendar'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS calendar;"
if [ -f calendar.txt ]
then
    columns=$(head -1 calendar.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/service_id TEXT/service_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
    fgrep -v service_id calendar.txt > calendar-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE calendar ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import calendar-wo-header.txt calendar"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE calendar ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE calendar ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE calendar ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE calendar ADD ptna_comment    TEXT DEFAULT '';"
    rm -f calendar-wo-header.txt
else
    columns="service_id TEXT PRIMARY KEY,monday INTEGER DEFAULT 0,tuesday INTEGER DEFAULT 0,wednesday INTEGER DEFAULT 0,thursday INTEGER DEFAULT 0,friday INTEGER DEFAULT 0,saturday INTEGER DEFAULT 0,sunday INTEGER DEFAULT 0,start_dateTEXT DEFAULT '',end_dateTEXT DEFAULT '', ptna_changedate TEXT DEFAULT '', ptna_is_invalid TEXT DEFAULT '', ptna_is_wrong TEXT DEFAULT '', ptna_comment TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE calendar ($columns);"
fi


#
# feed_info.txt - we take it as it is, no PRIMARY KEY defined, ...
#

echo "Table 'feed_info'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS feed_info;"
if [ -f feed_info.txt ]
then
    sqlite3 $SQ_OPTIONS $DB ".import feed_info.txt feed_info"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE feed_info ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE feed_info ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE feed_info ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE feed_info ADD ptna_comment    TEXT DEFAULT '';"
else
    columns="feed_publisher_name TEXT DEFAULT '',feed_publisher_url TEXT DEFAULT '',feed_lang TEXT DEFAULT '',feed_start_date TEXT DEFAULT '',feed_end_date TEXT DEFAULT '',feed_version TEXT DEFAULT '', ptna_changedate TEXT DEFAULT '', ptna_is_invalid TEXT DEFAULT '', ptna_is_wrong TEXT DEFAULT '', ptna_comment TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE feed_info ($columns);"
fi


#
# routes.txt - route_id is PRIMARY KEY, ...
#

echo "Table 'routes'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS routes;"
if [ -f routes.txt ]
then
    columns=$(head -1 routes.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/route_id TEXT/route_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
    fgrep -v route_id routes.txt > routes-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE routes ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import routes-wo-header.txt routes"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE routes ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE routes ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE routes ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE routes ADD ptna_comment    TEXT DEFAULT '';"
    rm -f routes-wo-header.txt
fi


#
# shapes.txt - shape_id is PRIMARY KEY, ...
#

echo "Table 'shapes'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS shapes;"
if [ -f shapes.txt ]
then
    columns=$(head -1 shapes.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/[\r\n]//gi')
    fgrep -v shape_id shapes.txt > shapes-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE shapes ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import shapes-wo-header.txt shapes"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE shapes       ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE shapes       ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE shapes       ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE shapes       ADD ptna_comment    TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "CREATE INDEX idx_shape_id ON  shapes (shape_id);"
    sqlite3 $SQ_OPTIONS $DB "UPDATE ptna SET has_shapes=(SELECT COUNT(*) FROM shapes);"
    rm -f shapes-wo-header.txt
else
    columns="shape_id TEXT DEFAULT '',shape_pt_lat TEXT DEFAULT '',shape_pt_lon TEXT DEFAULT '',shape_pt_sequence TEXT DEFAULT '', ptna_changedate TEXT DEFAULT '', ptna_is_invalid TEXT DEFAULT '', ptna_is_wrong TEXT DEFAULT '', ptna_comment TEXT DEFAULT ''"
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE shapes ($columns);"
fi


#
# stops.txt - stop_id is PRIMARY KEY, ...
#

echo "Table 'stops'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS stops;"
if [ -f stops.txt ]
then
    columns=$(head -1 stops.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/stop_id TEXT/stop_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
    fgrep -v stop_id stops.txt > stops-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE stops ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import stops-wo-header.txt stops"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE stops ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE stops ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE stops ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER TABLE stops ADD ptna_comment    TEXT DEFAULT '';"
    rm -f stops-wo-header.txt
fi


#
# stop_times.txt - we take it as it is, no PRIMARY KEY defined, ...
#

echo "Table 'stop_times'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS stop_times;"
if [ -f stop_times.txt ]
then
    columns=$(head -1 stop_times.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/[\r\n]//gi')
    fgrep -v stop_id stop_times.txt > stop_times-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE stop_times ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import stop_times.txt stop_times"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE stop_times  ADD ptna_changedate TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE stop_times  ADD ptna_is_invalid TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE stop_times  ADD ptna_is_wrong   TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE stop_times  ADD ptna_comment    TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "CREATE INDEX idx_trip_id ON  stop_times (trip_id);"
    rm -f stop_times-wo-header.txt
fi


#
# trips.txt - trip_id is PRIMARY KEY, ...
#

echo "Table 'trips'"

sqlite3 $SQ_OPTIONS $DB "DROP TABLE IF EXISTS trips;"
if [ -f trips.txt ]
then
    columns=$(head -1 trips.txt | sed -e 's/^\xef\xbb\xbf//' -e 's/\"//gi' -e 's/,/ TEXT, /g' -e 's/trip_id TEXT/trip_id TEXT PRIMARY KEY/' -e 's/[\r\n]//gi')
    fgrep -v trip_id trips.txt > trips-wo-header.txt
    sqlite3 $SQ_OPTIONS $DB "CREATE TABLE trips ($columns TEXT);"
    sqlite3 $SQ_OPTIONS $DB ".import trips-wo-header.txt trips"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE trips        ADD ptna_changedate      TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE trips        ADD ptna_is_invalid      TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE trips        ADD ptna_is_wrong        TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "ALTER  TABLE trips        ADD ptna_comment         TEXT DEFAULT '';"
    sqlite3 $SQ_OPTIONS $DB "CREATE INDEX idx_route_id ON  trips (route_id);"
     rm -f trips-wo-header.txt
fi

sqlite3 $SQ_OPTIONS $DB ".schema"

echo "Test for route_id from routes"
sqlite3 $SQ_OPTIONS $DB "SELECT route_id FROM routes WHERE route_id='1';"

echo "Test for trip_id from trips"
sqlite3 $SQ_OPTIONS $DB "SELECT trip_id FROM trips WHERE trip_id='1';"

echo "Test for route_id and trip_id from trips"
sqlite3 $SQ_OPTIONS $DB "SELECT route_id,trip_id FROM trips WHERE trip_id='1';"

echo
echo "OSM settings"
sqlite3 $SQ_OPTIONS $DB "SELECT * FROM osm;"
echo
echo "PTNA settings"
sqlite3 $SQ_OPTIONS $DB "SELECT * FROM ptna;"
