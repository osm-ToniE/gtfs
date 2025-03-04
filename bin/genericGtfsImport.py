#!/usr/bin/env python3

# Copyright 2025 Nitai Sasson
# Licensed under GNU GPLv3 or later

import sqlite3
import os
import re
import sys
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
    def __init__(self, gtfs_feed, dbfile, config={}):
        self.gtfs_feed = gtfs_feed
        self._con = sqlite3_connect_read_only(dbfile)
        self._con.row_factory = sqlite3.Row
        self.config_init()
        self.configure(config)

    def __del__(self):
        self._con.close()

    def get_cursor(self):
        """Get a new cursor for the sqlite3 database"""
        return self._con.cursor()

    def get_gtfs_feed(self, *args, **kwargs):
        return self.gtfs_feed

    def config_init(self):
        """Initialize default configuration for this importer"""
        self._config = {
            'get_trips': False, # Whether to fetch trips. Automatically overridden to True if any property in route_properties is a string starting with "trip_"
            'get_stops': False, # Whether to fetch a tuple of stops for each trip. Has no effect if get_trips is False. Automatically overridden to True if 'from' or 'to' use stop_name in route_properties
            'route_properties': {
                # define how each CSV field is determined
                # can be a GTFS field name (str) or a callable
                # if it's a route field name (e.g. route_short_name), it is used as-is
                # if it's a trip field name (e.g. trip_headsign), all unique non-empty values from trips are taken, sorted alphabetically, and joined with '|'
                # e.g. 'ref': 'trip_headsign' might give something like "4|4A" if some trips have 4 and others have 4A
                'ref': 'route_short_name',
                'type': 'osm_route',

                'trip_headsign': 'trip_headsign', # testing

                # 'from' and 'to' have special handling for the following values:
                # 'trip_headsign' - uses only trips with direction_id = 0 for 'to', direction_id = 1 for 'from'
                # 'stop_name' - uses the stop_name for the first stop of each trip for 'from', the last stop for 'to'
                # 'trip_headsign|stop_name' - concatenation of the two options above
                # note that 'from': 'stop_name' will give the first stop for *all* trips, with any direction_id, which is different from the behavior of 'from': 'trip_headsign'
                'from': 'trip_headsign|stop_name', # should this be the default?
                'to': 'trip_headsign|stop_name', # should this be the default?

                'comment': 'route_long_name',
                'operator': 'agency_name',

                # callable - receives two positional arguments, route (sqlite3.Row) and list of trips (either sqlite3.Row or dict, see get_trips documentation)
                'gtfs_feed': self.get_gtfs_feed,
                'route_id': 'route_id',
            }
        }

    def configure(self, config):
        """Configure importer options using a dictionary of configuration options.

        Options available: TBD...
        """
        # TODO: actually handle the input smartly
        self._config.update(config)

    def main(self, out_file):
        """Query database, convert to routes, sort and create .json"""
        # TODO: filter by stops?
        # TODO: optionally get trips for a route
        self.get_routes()
        self.sort_routes()
        self.output_routes(out_file)

    def get_routes(self):
        """Create self.routes list from database data.

        Calls self.make_route for each GTFS route in the database."""
        # get trips if needed
        if self.should_get_trips():
            trips_by_route_id = self.get_trips()
        else:
            trips_by_route_id = defaultdict(list) # fallback so later code can rely on it

        # get routes
        cur = self.get_cursor()
        res = cur.execute("""
            SELECT routes.*, agency_name, osm_route, sort_key
            FROM routes
            LEFT NATURAL JOIN agency
            LEFT NATURAL JOIN gtfs_route_types
            ;
            """)

        self.routes = []
        self._route_sort_key_by_route_id = {}
        for gtfs_route in res:
            route = self.make_route(gtfs_route, trips_by_route_id[gtfs_route['route_id']])
            self._route_sort_key_by_route_id[route['route_id']] = gtfs_route['sort_key']
            if route.get('ref') and route.get('type'):
                self.routes.append(route)
            else:
                print("Error: route without ref and type:")
                print(route)

    def get_trips(self):
        """Get trips from the database.

        Return value is a dict where the key is route_id and the value is a list of trip objects.
        Trip objects are sqlite3.Row if they do not include stops, or dict if they do.
        If they include stops, they have a new key called 'stops' which contains a list of sqlite3.Row objects
        with the fields: trip_id, stop_sequence, stops.*
        """
        cur = self.get_cursor()
        res = cur.execute("SELECT * FROM trips")

        trips_by_route_id = defaultdict(list)
        for trip in res:
            trips_by_route_id[trip['route_id']].append(trip)

        if not self.should_get_stops():
            return trips_by_route_id

        # get stops for each trip
        res = cur.execute("""
            SELECT trip_id, stop_sequence, stops.*
            FROM stop_times
            LEFT NATURAL JOIN stops
            ;
            """)

        stops_by_trip_id = defaultdict(dict)
        for stop in res:
            trip_seq_dict = stops_by_trip_id[stop['trip_id']]
            trip_seq_dict[int(stop['stop_sequence'])] = stop

        for trips in trips_by_route_id.values():
            for i, trip in enumerate(trips):
                trip = dict(trip) # convert sqlite3.Row to dict
                trips[i] = trip # replace Row with new dict
                trip_id = trip['trip_id']
                trip_stops = stops_by_trip_id[trip_id]
                trip['stops'] = [trip_stops[j] for j in sorted(trip_stops.keys())]
        return trips_by_route_id

    def should_get_trips(self):
        if self._config['get_trips']:
            return True
        return any(isinstance(s, str) and ('trip_' in s or 'stop_' in s) for s in self._config['route_properties'].values())

    def should_get_stops(self):
        if self._config['get_stops']:
            return True
        return any(isinstance(s, str) and 'stop_' in s for s in self._config['route_properties'].values())

    def sort_routes(self):
        """Sort self.routes."""
        self.routes.sort(key=self._make_sort_key())

    @staticmethod
    def sort_key(route):
        """Defines the sort order for routes with the same type.

        Override this function to customize sort order. See documentation of 'key' argument for details:
        https://docs.python.org/3/library/stdtypes.html#list.sort
        If you override, make sure to use the @staticmethod decorator, or add the self argument.

        Note: routes will be sorted by type according to a pre-defined order. This key is only used to decide the order of routes with the same type.
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

        Do not override this method - instead configure its behaviour using other mechanisms (TBD)
        Arguments:
        gtfs_route -- sqlite3.Row for the GTFS route
        gtfs_trips -- (optional) list of trips (each being sqlite3.Row (though this might change to dict)) that belong to this route

        Return value:
        dict representing this route as it should appear in the CSV.
        """
        route = {}
        for csv_field, source in self._config['route_properties'].items():
            if not source:
                continue
            if isinstance(source, str):
                # source is a GTFS field name or a '|'-separated list of GTFS field names
                values = []
                source = source.split('|')
                for src in source:
                    try:
                        # route field, e.g. route_short_name
                        values.append(gtfs_route[src].strip())
                    except IndexError:
                        if csv_field in ['from', 'to'] and src in ["trip_headsign", "stop_name"]:
                            # special handling code
                            if src == "stop_name":
                                i = 0 if csv_field == 'from' else -1
                                options = set(trip['stops'][i]['stop_name'].strip() for trip in gtfs_trips)
                                values.append('|'.join(sorted(options)))
                            else: # trip_headsign
                                direction_id = '1' if csv_field == 'from' else '0'
                                options = set(trip['trip_headsign'] for trip in gtfs_trips if trip['direction_id'] == direction_id)
                                values.append('|'.join(sorted(options)))
                        else:
                            try:
                                options = set(trip[src].strip() for trip in gtfs_trips)
                                values.append('|'.join(sorted(options)))
                            except IndexError:
                                print(f"Error: Field not found in route or trip: {src!r}")
                route[csv_field] = '|'.join([v for v in values if v])
            else:
                route[csv_field] = source(gtfs_route, gtfs_trips)

        # delete None/empty values
        for k in list(route.keys()):
            if not route[k]:
                del route[k]
        return route

    def output_routes(self, out_file):
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(self.routes, f, ensure_ascii=False)

# everything beyond this point is just playing around for now

importer = PtnaRoutesImporter(sys.argv[1], sys.argv[2])

importer.main(sys.argv[3])

sys.exit()
