#!/usr/bin/env python3

# Copyright 2025 Nitai Sasson
# Licensed under GNU GPLv3 or later

import sqlite3
import os
import re
import argparse
import json
from collections import defaultdict

def sqlite3_connect_read_only(filename):
    # use the recipe at https://www.sqlite.org/uri.html#the_uri_path to open the file with a read-only URI
    # I really don't care if I got it to work right in Windows...
    uri = filename
    uri = uri.replace("?", "%3f")
    uri = uri.replace("#", "%23")
    if os.name == 'nt':
        uri = uri.replace("\\", "/")
    uri = re.sub("/+", "/", uri)
    if os.name == 'nt' and re.match(r"[a-zA-Z]:/", uri):
        uri = "/" + uri
    uri = "file:" + uri + "?mode=ro"

    print(f"sqlite3.connect({uri!r}, uri=True)")
    return sqlite3.connect(uri, uri=True)

class PtnaRoutesImporter:
    """Class to import routes from GTFS and output them in .json to be injected to PTNA CSV.

    Can be used standalone, or subclassed and extended.
    """

    routes_query = """
        SELECT *
        FROM routes
        LEFT NATURAL JOIN agency
        LEFT NATURAL JOIN gtfs_route_types
        ;
        """
    """SQL query to get routes - must produce exactly one row per route, must include route_id"""


    trips_query = """
        SELECT * FROM trips;
        """
    """SQL query to get trips - must produce exactly one row per trip, must include trip_id, route_id"""

    stop_times_query = """
        SELECT trip_id, stop_sequence, stops.*
        FROM stop_times
        LEFT NATURAL JOIN stops
        ;
        """
    """SQL query to get stop_times - must produce exactly one row per stop_time, must include trip_id, stop_sequence"""

    def __init__(self, gtfs_feed, dbfile):
        self.gtfs_feed = gtfs_feed
        self._con = sqlite3_connect_read_only(dbfile)
        self._con.row_factory = sqlite3.Row
        self.route_properties = {}
        self.add_default_route_properties()

    def __del__(self):
        self._con.close()

    def get_cursor(self):
        """Get a new cursor for the sqlite3 database"""
        return self._con.cursor()

    def get_sql_routes(self):
        """Queries SQL and returns an iterable of of route rows.

        You can override this to use custom logic, but for SQL query tweaks you should simply override the routes_query attribute.
        """
        print("Running SQL query for routes:")
        print(self.routes_query)
        cur = self.get_cursor()
        return cur.execute(self.routes_query)

    def get_sql_trips(self):
        """Queries SQL and returns an iterable of of trip rows.

        You can override this to use custom logic, but for SQL query tweaks you should simply override the trips_query attribute.
        """
        print("Running SQL query for trips:")
        print(self.trips_query)
        cur = self.get_cursor()
        return cur.execute(self.trips_query)

    def get_sql_stop_times(self):
        """Queries SQL and returns an iterable of of stop_time rows.

        You can override this to use custom logic, but for SQL query tweaks you should simply override the stop_times_query attribute.
        """
        print("Running SQL query for stop_times/stops:")
        print(self.stop_times_query)
        cur = self.get_cursor()
        return cur.execute(self.stop_times_query)

    def get_gtfs_feed(self, *args, **kwargs):
        return self.gtfs_feed

    def add_default_route_properties(self):
        self.set_route_property("ref", "route_short_name")
        self.set_route_property("type", "osm_route")
        self.set_route_property("route_type", "route_type")
        self.set_route_property("route_type_name", "string")
        # self.set_route_property("from", "trip_headsign|stop_name")
        # self.set_route_property("to", "trip_headsign|stop_name")
        # self.set_route_property("comment", "route_long_name")
        self.set_route_property("operator", "agency_name")
        self.set_route_property("gtfs_feed", self.get_gtfs_feed)
        self.set_route_property("route_id", "route_id")

    delims = '|,;'
    """Available delimiters in properties. The first character is the default delimiter."""

    def split_source(self, source, property_name=None):
        """Split a source string into fields and delimiter"""
        # source is a GTFS field name or a delimiter-separated list of GTFS field names
        split = re.split(f"([{re.escape(self.delims)}])", source)

        fields = [f.strip() for f in split[::2] if f.strip()]

        seps = split[1::2] or [self.delims[0]]
        if len(set(seps)) > 1: raise ValueError(f"Only one of the delimiters {self.delims} can be used in a property. Not allowed: {property_name + ' = ' if property_name else ''}{source}")
        delim = seps[0]

        return fields, delim

    def set_route_property(self, property_name, source, get_trips=False, get_stops=False):
        """Define a property for a route in the output JSON. Can override a previously-defined property.

        Arguments:
            property_name - name of the property, e.g. 'ref', 'from', or something custom like 'city'
            source - either the name of one or more SQL columns, or a function.
                If it's a function, the function accepts these positional arguments:
                    property_name - the requested property, i.e. the property_name argument given to set_route_property
                    gtfs_route - SQL row of the route
                    gtfs_trips - iterable of trip SQL rows that belong to this route - present only if get_trips is True
                        if get_stops is True, each trip is guaranteed to have an additional `stops` key with a list of stops, in order, that the trip serves
                Otherwise, it should be a string of one or more SQL column names. You can use columns from routes, trips, or stops.
                You can specify multiple columns separated with | for example: "route_short_name|trip_headsign"
                When multiple values are found (either because multiple columns were specified, or different values were found for different trips/stops for the route), they are sorted alphabetically and separated with |
                for example: 24|24A|24B
            get_trips - whether the source function expects the trips argument to be present. Only relevant if source is a function.
            get_stops - whether the source function expects each trip to have a list of stops under a 'stops' key. Cannot be True if get_trips is False.

        If property_name is "from" or "to", SQL columns are handled with special logic - see _from_to_handler for details.
        """
        if not isinstance(property_name, str):
            raise TypeError("property_name must be a string")

        if isinstance(source, str):
            if not source:
                raise ValueError("source cannot be an empty string. use remove_route_property to remove a property")
            self.split_source(source, property_name) # just to raise errors
            self.route_properties[property_name] = source
        elif callable(source):
            if get_stops and not get_trips:
                raise ValueError("get_stops requires get_trips")
            self.route_properties[property_name] = (source, bool(get_trips), bool(get_stops))
        else:
            raise TypeError("source must be a string or a function")

    def remove_route_property(self, property_name):
        """Remove a route property_name - basically undo set_route_property"""
        del self.route_properties[property_name]

    def _get_property(self, gtfs_route, gtfs_trips, property_name, source):
        if isinstance(source, str):
            fields, delim = self.split_source(source)

            values = set()
            for field in fields:
                if property_name in ['from', 'to']:
                    # special case
                    if vals := self._from_to_handler(property_name, field, gtfs_route, gtfs_trips):
                        values |= vals
                        continue
                if field in gtfs_route.keys():
                    # routes column
                    values.add(gtfs_route[field])
                    continue
                # not a routes column, try trips
                for trip in gtfs_trips:
                    if field in trip.keys():
                        values.add(trip[field])
                        continue
                    # not a trips column, must be a stops column
                    for stop in trip['stops']:
                        if field in stop.keys():
                            values.add(stop[field])
                            continue
                        # this is supposed to be caught earlier but might as well raise it here if it managed to get this far
                        raise KeyError(f"Column {field!r} for route property {property_name!r} could not be found in any of the SQL queries. "
                            "The available columns for routes, trips, and stops respectively:",
                            list(gtfs_route.keys()), list(trip.keys()), list(stop.keys()))

            values = {v.strip() for v in values if v and v.strip()}
            return delim.join(sorted(values))
        else:
            # source is a function
            src, get_trips, get_stops = source
            if get_trips:
                return src(property_name, gtfs_route, gtfs_trips)
            else:
                return src(property_name, gtfs_route)

    def _from_to_handler(self, property_name, field, gtfs_route, gtfs_trips):
        """Special logic to make more useful values for "from" and "to" fields"""
        assert property_name in ['from', 'to']
        match field:
            case 'trip_headsign':
                # 'to' takes trip_headsign from trips going the normal way
                # 'from' takes from trips going the other way
                direction_id = '1' if property_name == 'from' else '0'
                return set(trip['trip_headsign'] for trip in gtfs_trips if trip['direction_id'] == direction_id)
            case 'stop_name':
                # 'to' takes stop_name from the last stop in each trip
                # 'from' takes from the first stop
                i = 0 if property_name == 'from' else -1
                return set(trip['stops'][i]['stop_name'] for trip in gtfs_trips)
            case _:
                # no special handling
                return None

    def main(self, out_file):
        """Query database, convert to routes, sort and create .json"""
        # TODO: filter by stops?
        print("Starting import process")
        self.get_routes()
        self.sort_routes()
        self.output_routes(out_file)

    def get_routes(self):
        """Create self.routes list from database data.

        Calls self.make_route for each GTFS route in the database."""

        sql_routes, trips_by_route_id = self._get_routes_and_trips()

        print("Converting routes...")
        self.routes = []
        self._route_sort_key_by_route_id = {}
        for gtfs_route in sql_routes:
            route = self.make_route(gtfs_route, trips_by_route_id[gtfs_route['route_id']])
            self._route_sort_key_by_route_id[route['route_id']] = gtfs_route['sort_key']
            if route.get('ref') and route.get('type'):
                self.routes.append(route)
            else:
                print("Error: route without ref and type:")
                print(route)
                print(f"from: {dict(gtfs_route)}")
        print(f"Got {len(self.routes)} routes")

    def _get_routes_and_trips(self):
        """Returns an iterable of routes, and - if required - a mapping from route_id to list of trips.

        Determines whether trips and stops are required based on the defined route properties."""
        sql_routes = self.get_sql_routes()
        sql_routes_columns = [c[0] for c in sql_routes.description]

        # We start by assuming we don't need trips or stops
        trips_cur = None
        trips_columns = None
        stops_cur = None
        stops_columns = None

        print("Preparing required data based on route properties")

        for prop_name, source in self.route_properties.items():
            if isinstance(source, str):
                # source is SQL column(s)
                print(f"{prop_name} = {source}")
                for field in self.split_source(source)[0]:
                    if field in sql_routes_columns:
                        print(f"\t{field} found in routes query")
                        continue

                    # not a route column, we need trips
                    trips_cur = trips_cur or self.get_sql_trips()
                    trips_columns = trips_columns or [c[0] for c in trips_cur.description]
                    if field in trips_columns:
                        print(f"\t{field} found in trips query")
                        continue

                    # not a trip column, we need stops
                    stops_cur = stops_cur or self.get_sql_stop_times()
                    stops_columns = stops_columns or [c[0] for c in stops_cur.description]
                    if field in stops_columns:
                        print(f"\t{field} found in stop_times/stops query")
                        continue

                    raise RuntimeError(f"Column {field!r} for route property {prop_name!r} could not be found in any of the SQL queries. "
                        "The available columns for routes, trips, and stops respectively:",
                        sql_routes_columns, trips_columns, stops_columns)
            else:
                # callable, just figure out if we need trips and stops
                f, get_trips, get_stops = source
                print(f"{prop_name} = {f}, {'with' if get_trips else 'without'} trips and {'with' if get_stops else 'does not require' if get_trips else 'without'} stops")
                assert get_trips or not get_stops # can't get stops without trips, should have raised an error before reaching here
                if get_trips:
                    trips_cur = trips_cur or self.get_sql_trips()
                if get_stops:
                    stops_cur = stops_cur or self.get_sql_stop_times()

        trips_by_route_id = self.get_trips(trips_cur, stops_cur)
        return sql_routes, trips_by_route_id

    def get_trips(self, trips_cur, stops_cur):
        """Get trips from the database.

        Return value is a dict where the key is route_id and the value is a list of trip mappings.
        Trip mappings are sqlite3.Row if they do not include stops, or dict if they do.
        If they include stops, they have a new key called 'stops' which contains a list of sqlite3.Row objects
        with the fields from stop_times_query
        """
        trips_by_route_id = defaultdict(list)

        if not trips_cur:
            # no action needed
            print("Skipping trips and stops queries - not required")
            return trips_by_route_id

        print("Getting trip data from query...")
        for trip in trips_cur:
            trips_by_route_id[trip['route_id']].append(trip)

        print(f"Got {sum(len(ts) for ts in trips_by_route_id.values())} trips for {len(trips_by_route_id)} route_ids")

        if not stops_cur:
            # job done
            print("Skipping stops query - not required")
            return trips_by_route_id

        # get stops for each trip
        print("Getting stop_time/stop data from query...")
        stops_by_trip_id = defaultdict(dict)
        for stop in stops_cur:
            trip_seq_dict = stops_by_trip_id[stop['trip_id']]
            trip_seq_dict[int(stop['stop_sequence'])] = stop

        print(f"Got {sum(len(ss) for ss in stops_by_trip_id.values())} stop_times for {len(stops_by_trip_id)} trip_ids")

        # give each trip its stops list
        for trips in trips_by_route_id.values():
            for i, trip in enumerate(trips):
                trip = dict(trip) # convert sqlite3.Row to dict
                trips[i] = trip # replace Row with new dict
                trip_id = trip['trip_id']
                trip_stops = stops_by_trip_id[trip_id]
                trip['stops'] = [trip_stops[j] for j in sorted(trip_stops.keys())]
        return trips_by_route_id

    def sort_routes(self):
        """Sort self.routes."""
        print("Sorting routes")
        self.routes.sort(key=self._make_sort_key())

    @staticmethod
    def sort_key(route):
        """Defines the sort order for routes with the same type.

        Override this function to customize sort order. See documentation of 'key' argument for details:
        https://docs.python.org/3/library/stdtypes.html#list.sort
        If you override, make sure to use the @staticmethod decorator, or add the self argument.

        Note: routes will be sorted by type according to a pre-defined order, roughly: train, subway, tram, bus, trolleybus, monorail, funicular, aerialway, ferry.
        This key is only used to decide the order of routes with the same type.
        Generally, routes of different types would never be put in the same import statement anyway.
        """
        # separate numbers and non-numbers in ref
        # examples:
        # "532" -> ['', '532', '']
        # "A" -> ['A']
        # "T53-E2" -> ['T', '53', '-E', '2', '']
        # "2B" -> ['', '2', 'B']
        split_ref = re.split(r'(\d+)', route['ref'])
        # convert number strings to numbers
        # example:
        # ['T', '53', '-E', '2', ''] -> ['T', 53, '-E', 2, '']
        split_ref[1::2] = [int(x) for x in split_ref[1::2]]

        return (split_ref, route['ref'], route.get('operator'), route['route_id'])
        # route['ref'] is used to be consistent between 003, 03 and 3

    def _make_sort_key(self):
        """Prepend type_sort_key to the sort key returned by sort_key.

        Do not override this method! Override sort_key instead.
        """
        return lambda route: (self._route_sort_key_by_route_id[route['route_id']], self.sort_key(route))

    def make_route(self, gtfs_route, gtfs_trips=[]):
        """Convert a GTFS route to a CSV route.

        Do not override this method - instead configure its behaviour using set_route_property
        Arguments:
        gtfs_route -- sqlite3.Row for the GTFS route
        gtfs_trips -- (optional) list of trips (each being sqlite3.Row (though this might change to dict)) that belong to this route

        Return value:
        dict representing this route as it should appear in the output JSON.
        """
        route = {}
        for property_name, source in self.route_properties.items():
            route[property_name] = self._get_property(gtfs_route, gtfs_trips, property_name, source)

        # delete None/empty values
        for k in list(route.keys()):
            if not route[k]:
                del route[k]
        return route

    def output_routes(self, out_file):
        print(f"Writing routes to {out_file}")
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(self.routes, f, ensure_ascii=False)


# Commandline script

class PropertyParseAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        values = {k: v for k, v in (v.split("=", maxsplit=1) for v in values)}
        setattr(namespace, self.dest, values)

def main():
    parser = argparse.ArgumentParser(epilog='''Example:
  %(prog)s --database ptna-gtfs-sqlite.db --gtfs-feed CA-QC-RTC --outfile routes.json comment=route_long_name from="trip_headsign|stop_name" to="trip_headsign|stop_name"''', formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--database', required=True, help='sqlite3 database file created by PTNA')
    parser.add_argument('-g', '--gtfs-feed', required=True, help='feed identifier - value for gtfs_feed')
    parser.add_argument('-o', '--outfile', required=True, help='routes output file (.json)')
    parser.add_argument("properties", metavar='property=sql-column', nargs="*", action=PropertyParseAction, help="output route property and its associated source column, see example")

    args = parser.parse_args()

    importer = PtnaRoutesImporter(args.gtfs_feed, args.database)

    for prop, source in args.properties.items():
        if source:
            importer.set_route_property(prop, source)
        else:
            importer.remove_route_property(prop)

    importer.main(args.outfile)

if __name__ == "__main__":
    main()
