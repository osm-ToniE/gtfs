#!/usr/bin/env --split-string=sh -c '"$(dirname -- "$0")/venv/bin/python3" "$0" "$@"'
# shebang to run with the venv in this directory
# run create-venv.sh to create it

# Copyright 2025 Nitai Sasson
# Licensed under GNU GPLv3 or later

# This script creates a .json file with an array of routes (catalog entries) in the provided region.
# The output must be sorted in the order that items should appear on PTNA (sorted by type, then by ref)

import csv
import re
import argparse
import json
import shapely
from os import path
from collections import Counter, defaultdict

def main(shape_file, trains, gtfs_dir, out_file):
    use_shape = not trains
    # Use a polygon to select stops
    shape = None
    if use_shape:
        shape = get_shape(shape_file)
        print(f'get_shape returned a shape with {shapely.get_num_geometries(shape)} geometries and {shapely.get_num_coordinates(shape)} coordinates')

    # Find routes that stop at these stops
    # (this section is to be replaced with some SQL magic)
    train_data = {}
    stop_ids, all_stops = stop_ids_from_shape(gtfs_dir, shape, trains, train_data)
    print(f"{len(stop_ids)} stop_ids, {len(train_data['stops_by_stop_id'])} train stops out of stop_ids_from_shape")
    internal_trip_ids, connecting_trip_ids, cities_by_trip_id = trip_ids_from_stop_ids(gtfs_dir, stop_ids, train_data, all_stops)
    print(f"{len(internal_trip_ids)} internal trips, {len(connecting_trip_ids)} connecting trips, {len(train_data['sequence_by_trip_id'])} train sequences out of trip_ids_from_stop_ids")
    route_info, internal_route_ids = route_ids_from_trip_ids(gtfs_dir, internal_trip_ids, connecting_trip_ids, train_data, cities_by_trip_id)
    print(f"{len(route_info)} route_ids out of route_ids_from_trip_ids, of which {len(internal_route_ids)} are internal. Also {len(train_data['trip_by_trip_id'])} train trips.")
    routes = routes_from_route_ids(gtfs_dir, route_info)
    print(f"{len(routes)} routes out of routes_from_route_ids")
    populate_agency_name_for_routes(gtfs_dir, routes)

    # print some statistics
    print("Operators breakdown:")
    for operator, count in Counter(route['agency_name'] for route in routes).most_common():
        print(count, operator)

    process_train_data(train_data)

    routes_by_catalog_number = group_routes_by_catalog_number(routes, train_data)
    print(f"{len(routes_by_catalog_number)} catalog numbers from routes_by_catalog_number")
    catalog = create_ptna_routes(routes_by_catalog_number, internal_route_ids)
    print(f"{len(catalog)} routes in the final catalog, of which {sum(1 for r in catalog if r['internal'] == 'yes')} are internal")
    sort_catalog(catalog)
    print(f"Saving routes to {out_file}")
    dump_catalog(catalog, out_file)

### GTFS parsing - this part should be replaced with some SQL queries ###
def fix_gtfs_name(s):
    # GTFS has bad names
    s = s.replace("''", '"')
    s = s.replace('""', '"') # one occurrence of this (גן טכנולוגי/א''''ס הפועל)
    s = s.replace('\xa0', ' ') # non-breaking space
    s = s.strip()
    if not s: return s
    s = re.sub(' +', ' ', s)
    s = re.sub('(?<=[א-ת])"(?=[א-ת])', '״', s) # Hebrew Gershayim (note: א-ת includes אותיות סופיות)
    if s[0] == "'": # sometimes they put the Geresh on the wrong side, probably bad RTL support in their GUI
        s = s[1:] + "'"
    s = re.sub("(?<=[א-ת])'", '׳', s) # Hebrew Geresh
    return s


def stop_ids_from_shape(gtfs_dir, shape, trains, train_data):
    # return value:
    # 1. stop_ids: set of stop_ids within this shape
    # 2. all_stops: dict mapping stop_id to stop struct for all stops in GTFS
    # if trains is True, stop_ids will have the stop_ids of all train stations and nothing else (shape is not used)
    # this function also adds to train_data:
    # stops_by_stop_id: dict mapping stop_id to stop struct
    stop_ids = set()
    all_stops = {}
    train_stops_by_stop_id = {}
    if not trains:
        shapely.prepare(shape)
    print("Parsing stops.txt")
    with open(path.join(gtfs_dir, 'stops.txt'), newline='', encoding='utf_8_sig') as f:
        csv_reader = csv.DictReader(f)
        for stop in csv_reader:
            # add to all_stops with fixed name and added city field
            all_stops[stop['stop_id']] = stop
            stop['stop_name'] = fix_gtfs_name(stop['stop_name'])
            city_re = re.fullmatch(r'רחוב: .* עיר: (.*) רציף: .* קומה: .*', stop['stop_desc'])
            if city_re:
                stop['city'] = fix_gtfs_name(city_re.group(1))
            # add train stations to train_data
            if not stop['stop_desc']:
                # train station
                train_stops_by_stop_id[stop['stop_id']] = stop
                if trains:
                    stop_ids.add(stop['stop_id'])
            # add stops in the shape
            if not trains and shapely.contains_xy(shape, float(stop['stop_lon']), float(stop['stop_lat'])):
                stop_ids.add(stop['stop_id'])
            elif trains and re.match(r'רחוב: מסילת (ברזל |קו )', stop['stop_desc']):
                # add light rail stops as well
                # eventually replace this bullshit logic with querying route_type over SQL instead of going by stops
                # but right now, this is the easiest way
                stop_ids.add(stop['stop_id'])
    train_data['stops_by_stop_id'] = train_stops_by_stop_id
    return stop_ids, all_stops

def trip_ids_from_stop_ids(gtfs_dir, stop_ids, train_data, all_stops):
    # return values:
    # 1. internal_trip_ids - trips that only stop at the given stops
    # 2. connecting_trip_ids - trips that stop at the given stops as well as other stops (i.e. connect to other districts/regions)
    # 3. cities_by_trip_id - dict from trip_id to a set of city names in which the trip stops
    # this function also adds to train_data:
    # sequence_by_trip_id - dict mapping trip_id to a tuple of the stop_ids in its route
    in_trip_ids = set()
    out_trip_ids = set()
    train_trip_sequence_dict_by_trip_id = defaultdict(dict)
    cities_by_trip_id = defaultdict(set)
    print("Parsing stop_times.txt")
    with open(path.join(gtfs_dir, 'stop_times.txt'), newline='', encoding='utf_8_sig') as f:
        csv_reader = csv.DictReader(f)
        for stop_time in csv_reader:
            if 'city' in all_stops[stop_time['stop_id']]:
                cities_by_trip_id[stop_time['trip_id']].add(all_stops[stop_time['stop_id']]['city'])
            if stop_time['stop_id'] in train_data['stops_by_stop_id']:
                sequence_dict = train_trip_sequence_dict_by_trip_id[stop_time['trip_id']]
                if str(int(stop_time['stop_sequence'])) != stop_time['stop_sequence']:
                    print(f"Warning: weird number formatting '{stop_time['stop_sequence']}' in stop_sequence for trip_id {stop_time['trip_id']}")
                stop_seq = int(stop_time['stop_sequence'])
                assert stop_seq not in sequence_dict
                sequence_dict[stop_seq] = stop_time['stop_id']
            if stop_time['stop_id'] in stop_ids:
                in_trip_ids.add(stop_time['trip_id'])
            else:
                out_trip_ids.add(stop_time['trip_id'])
    internal_trip_ids = in_trip_ids - out_trip_ids
    connecting_trip_ids = in_trip_ids & out_trip_ids
    assert internal_trip_ids.isdisjoint(connecting_trip_ids)
    assert internal_trip_ids | connecting_trip_ids == in_trip_ids

    # convert sequence dicts to sequence tuples
    train_data['sequence_by_trip_id'] = {trip_id: tuple(sequence[i] for i in sorted(sequence.keys())) for trip_id, sequence in train_trip_sequence_dict_by_trip_id.items()}
    return internal_trip_ids, connecting_trip_ids, cities_by_trip_id

def route_ids_from_trip_ids(gtfs_dir, internal_trip_ids, connecting_trip_ids, train_data, cities_by_trip_id):
    # return values:
    # 1. route_info: a dict where the key is route_id and the value is a tuple: (direction_id, trip_headsign, cities)
    #    where cities is a frozenset of the names of cities the route visits
    # 2. internal_route_ids: set of route_ids belonging to internal routes
    # all routes have the same direction_id for all trips (base assumption for Israel GTFS)
    # almost all routes have the same trip_headsign for all trips, with the only exception being
    # if there is a route change that changes the headsign within the GTFS date range

    # this function also adds to train_data:
    # trip_by_trip_id: dict mapping trip_id to trip struct
    route_info = {}
    internal_route_ids = set()
    connecting_route_ids = set()
    logged_problem_routes = set()
    relevant_trip_ids = internal_trip_ids | connecting_trip_ids
    train_trip_by_trip_id = {}
    print("Parsing trips.txt")
    with open(path.join(gtfs_dir, 'trips.txt'), newline='', encoding='utf_8_sig') as f:
        csv_reader = csv.DictReader(f)
        for trip in csv_reader:
            if trip['trip_id'] in train_data['sequence_by_trip_id']:
                train_trip_by_trip_id[trip['trip_id']] = trip
            if trip['trip_id'] in relevant_trip_ids:
                destination = trip['trip_headsign'].replace('_', ' - ')

                trip_cities = frozenset(cities_by_trip_id[trip['trip_id']])
                route_cities = trip_cities
                # add trip cities to route_info if the route is already in route_info
                if trip['route_id'] in route_info:
                    # get union of route and trip cities and update route_info with the union
                    route_cities = route_info[trip['route_id']][2]
                    route_cities = route_cities | trip_cities
                    route_info[trip['route_id']] = route_info[trip['route_id']][:2] + (route_cities,)

                # sanity check - all trips of a single route are going to the same place
                if trip['route_id'] not in route_info or route_info[trip['route_id']][:2] == (trip['direction_id'], destination):
                    route_info[trip['route_id']] = (trip['direction_id'], destination, route_cities)
                else:
                    # route is most likely being changed (stops added/removed), so destination is different, nothing to do
                    # ensure direction is still the same, because that is an important base assumption in Israel's GTFS
                    assert route_info[trip['route_id']][0] == trip['direction_id'], f"Failed assumption in Israel GTFS: all trips of a route have the same direction. route_id = {trip['route_id']}"
                    if trip['route_id'] not in logged_problem_routes:
                        logged_problem_routes.add(trip['route_id'])
                        print(
                            "Route has trips with different headsigns in trips.txt: "
                            f"route_id = {trip['route_id']}, "
                            f"trip_headsign = {route_info[trip['route_id']][1]} or {destination}"
                        )
                    if not route_info[trip['route_id']][1]:
                        # better a non-blank destination than a blank one
                        route_info[trip['route_id']] = (trip['direction_id'], destination, route_cities)
                if trip['trip_id'] in internal_trip_ids:
                    internal_route_ids.add(trip['route_id'])
                else:
                    connecting_route_ids.add(trip['route_id'])
    train_data['trip_by_trip_id'] = train_trip_by_trip_id
    # if a route has both connecting and internal trips, consider it connecting
    # (should ~never happen because all trips of a route should have the same stops)
    internal_route_ids -= connecting_route_ids
    return route_info, internal_route_ids

def routes_from_route_ids(gtfs_dir, route_info):
    routes = []
    print("Parsing routes.txt")
    with open(path.join(gtfs_dir, 'routes.txt'), newline='', encoding='utf_8_sig') as f:
        csv_reader = csv.DictReader(f)
        for route in csv_reader:
            if route['route_id'] in route_info:
                route['direction_and_headsign'] = route_info[route['route_id']][:2]
                route['cities'] = route_info[route['route_id']][2]
                routes.append(route)
    return routes

def populate_agency_name_for_routes(gtfs_dir, routes):
    agency_names = {}
    print("Parsing agency.txt")
    with open(path.join(gtfs_dir, 'agency.txt'), newline='', encoding='utf_8_sig') as f:
        csv_reader = csv.DictReader(f)
        for agency in csv_reader:
            agency_names[agency['agency_id']] = agency['agency_name']
    for route in routes:
        route['agency_name'] = agency_names[route['agency_id']]
### end GTFS parsing ###

def get_shape(shape_file):
    # return a Shapely shape from a geojson file
    with open(shape_file, encoding='utf-8') as f:
        return shapely.from_geojson(f.read())

def group_routes_by_catalog_number(routes, train_data):
    train_sequence_by_route_id = {}
    for trip_id, trip in train_data['trip_by_trip_id'].items():
        seq = trip['sequence']
        if trip['direction_id'] == '1':
            # make reversed sequence distinct (just need equality comparison to fail)
            seq = ("reversed",) + seq
        train_sequence_by_route_id[trip['route_id']] = seq
    routes_by_catalog_number = {}
    for route in routes:
        desc_parts = route['route_desc'].split('-')
        # assume non-train routes
        if len(desc_parts) != 3:
            # train route
            route_id = route['route_id']
            route_identifier = train_data['identifier_by_route_id'][route_id]
            routes_list = routes_by_catalog_number.setdefault(route_identifier, [])
            seq = train_sequence_by_route_id[route_id]
            if not any(train_sequence_by_route_id[r['route_id']] == seq for r in routes_list):
                routes_list.append(route)
            continue

        catalog_number = desc_parts[0]
        if catalog_number == '11900':
            # these student lines all use the same catalog id for some reason
            catalog_number = f"{desc_parts[0]}-*-{desc_parts[2]}"

        # get or create catalog entry
        catalog_routes = routes_by_catalog_number.setdefault(catalog_number, [])
        catalog_routes.append(route)
    return routes_by_catalog_number

class InvalidRouteError(ValueError):
    # used for try-except
    pass

def create_ptna_routes(routes_by_catalog_number, internal_route_ids):
    # return a list of route objects (dicts)
    # normal fields:
    # ref; type; comment; from; to; operator; gtfs_feed; route_id
    # extra fields usable for filtering:
    # internal=yes/no: yes = this route is fully contained within this region; no = this route has stops outside this region
    # catalog_number: (all except train routes) the "makat" of the route, also appears in the comment
    # city: (all except train routes) comma-delimited list of cities that the route visits, in alphabetical order, e.g. "חיפה,נשר"
    # train_numbers: (train routes only) a human-readable list of number ranges of trains that serve this line, e.g. "401-409, 418-426, 429", also appears in the comment
    # train_numbers_full: (train routes only) a comma-separated list of train numbers, e.g. "401,402,403,404,405,406,407,409,418,419,420,421,422,423,424,425,426,429"
    # maybe in the future: number of stops, other metadata...
    osm_route_type_conversion = {
        '0': "light_rail",
        '2': "train",
        '3': "bus",
        '5': "tram", # incorrectly used for aerialway and funicular
        '6': "aerialway", # not used in source data
        '7': "funicular", # not used in source data
        '8': "share_taxi", # maybe?
        '715': "flexible" # no idea what this is
    }
    catalog = []
    for catalog_number, routes in routes_by_catalog_number.items():
        try:
            catalog_entry = {}
            # type (previously known as route_type)
            route_type = {route['route_type'] for route in routes}
            if len(route_type) != 1:
                raise InvalidRouteError(f"Multiple different route_type values for catalog number {catalog_number}: {route_type}")
            route_type = next(iter(route_type))
            if route_type == '5':
                # bad data
                if routes[0]['agency_id'] == '20':
                    # Carmelit - funicular
                    route_type = '7'
                elif routes[0]['agency_id'] == '33':
                    # Rachbalit - aerialway
                    route_type = '6'
                else:
                    print(f'Warning: Unrecognized "tram" line: {catalog_number}')
            if route_type not in osm_route_type_conversion:
                raise InvalidRouteError(f"Unrecognized route_type {route_type} for catalog number {catalog_number}")
            route_type = osm_route_type_conversion[route_type]
            catalog_entry['type'] = route_type

            train = route_type == 'train'

            if train:
                # unpack the route identifier
                ref, ptna_from, ptna_to, train_numbers, train_numbers_full = catalog_number

            # ref
            if train:
                catalog_entry['ref'] = str(ref)
            else:
                names = {route['route_short_name'] for route in routes}
                if len(names) == 1:
                    # just one name, it's the ref
                    catalog_entry['ref'] = next(iter(names))
                    if not re.match(r"\d+", catalog_entry['ref']):
                        raise InvalidRouteError(f"Expected route short name to start with number, catalog number {catalog_number} got {catalog_entry['ref']!r}")
                else:
                    # multiple names, should all start with same number - use that number as ref
                    new_name = None
                    for name in names:
                        name_candidate = re.match(r"\d+", name)
                        if not name_candidate:
                            raise InvalidRouteError(f"Expected route short name to start with number, catalog number {catalog_number} got {name!r}")
                        name_candidate = name_candidate.group()
                        if new_name and new_name != name_candidate:
                            raise InvalidRouteError(f"Expected all short names of a route to have the same number, catalog number {catalog_number} got {names}")
                        new_name = name_candidate
                    catalog_entry['ref'] = new_name

            # comment, catalog_number, and city
            if train:
                catalog_entry['comment'] = f"מספרי רכבות: {train_numbers}"
                catalog_entry['train_numbers'] = train_numbers
                catalog_entry['train_numbers_full'] = train_numbers_full
            else:
                comment = f"מק״ט: {catalog_number}"
                if route_type == 'bus':
                    real_catalog_number = re.match(r'\d*', catalog_number).group() # take just 11900 from 11900-*-*
                    comment += f" ([https://markav.net/line/{real_catalog_number}/ מר קו])"
                catalog_entry['comment'] = comment
                catalog_entry['catalog_number'] = catalog_number
                catalog_entry['city'] = ','.join(sorted(frozenset.union(*[route['cities'] for route in routes])))

            # internal
            catalog_entry['internal'] = "yes" if all(route['route_id'] in internal_route_ids for route in routes) else "no"

            # from and to
            if train:
                catalog_entry['to'] = fix_gtfs_name(ptna_to)
                catalog_entry['from'] = fix_gtfs_name(ptna_from)
            else:
                to_options = set()
                from_options = set()
                for route in routes:
                    direction, headsign = route['direction_and_headsign']
                    if direction not in ('0', '1'):
                        raise InvalidRouteError(f"Unrecognized direction {direction} for route {route}")
                    headsign = fix_gtfs_name(headsign)
                    if not headsign: continue
                    elif direction == '0':
                        to_options.add(headsign)
                    else:
                        from_options.add(headsign)
                catalog_entry['to'] = '|'.join(sorted(to_options))
                catalog_entry['from'] = '|'.join(sorted(from_options))
                # TO DO: add the actual stop names for first and last stop, as parsed from route_long_name

            # operator
            operator = {fix_gtfs_name(route['agency_name']) for route in routes}
            if len(operator) != 1:
                print(f"Warning: different operators for catalog number {catalog_number}: {operator}")
            catalog_entry['operator'] = sorted(operator)[0]

            # GTFS references
            catalog_entry['gtfs_feed'] = 'IL-MOT'
            catalog_entry['route_id'] = ';'.join(route['route_id'] for route in routes)

            catalog.append(catalog_entry)
            assert all(type(v) is str for v in catalog_entry.values())
            assert all(type(v) is str for v in catalog_entry.keys())
        except InvalidRouteError as err:
            print(f"Error: {err}")
    return catalog

def sort_catalog(catalog):
    route_type_order = {
        "train": 0,
        "light_rail": 1,
        "tram": 3,
        "aerialway": 4,
        "funicular": 5,
        "bus": 6,
        "share_taxi": 7,
        "flexible": 8
    }
    def sort_key(catalog_entry):
        ref = catalog_entry['ref']
        num = re.match(r"\d+", ref).group()
        return (route_type_order[catalog_entry['type']], int(num), catalog_entry['ref'])
    catalog.sort(key=sort_key)

def dump_catalog(catalog, out_file):
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(catalog, f, ensure_ascii=False)

### Train code - an absolute pain ###
def process_train_data(train_data):
    # the way this data needs to be juggled is mind-boggling
    # I advise you to trust that this works and never look at it again
    # especially because I have a terminal case of having to make everything in unreadable dict comprehension
    train_stops_by_stop_id = train_data['stops_by_stop_id']
    train_sequence_by_trip_id = train_data['sequence_by_trip_id']
    train_trip_by_trip_id = train_data['trip_by_trip_id']
    print(f"Processing train data: {len(train_stops_by_stop_id)} stops, {len(train_sequence_by_trip_id)} sequences, {len(train_trip_by_trip_id)} trips")
    # reverse trips that go the other way
    for trip_id, trip in train_trip_by_trip_id.items():
        if trip['direction_id'] == '1':
            train_sequence_by_trip_id[trip_id] = tuple(reversed(train_sequence_by_trip_id[trip_id]))

    # group sequences by ref without duplicates
    train_sequences_by_ref = {}
    for trip_id, trip in train_trip_by_trip_id.items():
        # trip_headsign is a number of 1 to 4 digits
        # the ref is the hundreds digit
        trip['train_number'] = int(trip['trip_headsign'])
        ref = (trip['train_number'] // 100) % 10
        trip['ref'] = ref
        trip['sequence'] = train_sequence_by_trip_id[trip_id]
        ref_sequences_set = train_sequences_by_ref.setdefault(ref, set())
        ref_sequences_set.add(trip['sequence'])

    # merge duplicates within each ref
    route_identifier_by_sequence_by_ref = {
        ref: map_sequence_to_route_identifier(ref, train_sequences, train_data)
            for ref, train_sequences in train_sequences_by_ref.items() }

    # map train number to route identifier, dict sorted by train number
    route_identifier_by_train_number = {
        trip['train_number']: route_identifier_by_sequence_by_ref[trip['ref']][trip['sequence']] for trip_id, trip in sorted(train_trip_by_trip_id.items(), key=lambda p: p[1]['train_number'])
    }

    # figure out train number ranges (human-readable string) for each route identifier
    numbers_by_route_identifier = generate_train_numbers_by_route_identifier(route_identifier_by_train_number)

    train_identifier_by_route_id = {}

    for trip in train_trip_by_trip_id.values():
        # look, this time I didn't do it in dict comprehension
        # aren't you proud?
        route_id = trip['route_id']
        route_identifier = route_identifier_by_train_number[trip['train_number']]
        train_identifier_by_route_id[route_id] = route_identifier + numbers_by_route_identifier[route_identifier]

    train_data['identifier_by_route_id'] = train_identifier_by_route_id

def generate_train_numbers_by_route_identifier(route_identifier_by_train_number):
    # return value is a dict mapping route_identifier to a string listing all the train numbers that use this route identifier
    # example string: "401-409, 418-426, 429"
    # also gives a simple comma-separated list of numbers: "401, 402, 403, 404, 405, 406, 407, 409, 418, 419, 420, 421, 422, 423, 424, 425, 426, 429"
    ranges_by_route_identifier = defaultdict(list)
    numbers_by_route_identifier = defaultdict(list)
    prev_identifier = None
    range_start = None
    prev_number = 0
    for train_number, route_identifier in route_identifier_by_train_number.items():
        if prev_identifier != route_identifier or (prev_number // 1000) != (train_number // 1000):
            # start of a new range
            if range_start: # don't enter this in the first iteration of the loop
                # add the range that just ended to the route_identifier it belongs to
                ranges_by_route_identifier[prev_identifier].append((range_start, prev_number))
            range_start = train_number
        prev_identifier = route_identifier
        prev_number = train_number
        numbers_by_route_identifier[route_identifier].append(train_number)
    ranges_by_route_identifier[prev_identifier].append((range_start, prev_number))

    return {
        route_identifier:
            (', '.join(
                str(range_start) if range_start == range_end else f"{range_start}-{range_end}"
                for range_start, range_end in ranges ), # human-readable list of ranges and individual numbers
            ','.join(str(num) for num in numbers_by_route_identifier[route_identifier])) # list of numbers, comma-delimited (no spaces)
        for route_identifier, ranges in ranges_by_route_identifier.items()
    }

def map_sequence_to_route_identifier(ref, train_sequences, train_data):
    # return value is a dict
    # keys are the elements of train_sequences (tuples of stop_id)
    # values are tuples of: (ref, origin, destination)

    # create initial state: each sequence is alone in a list
    sub_sequences_by_full_sequence = {seq: [seq] for seq in train_sequences}

    # this recipe works - I haven't tried removing steps to see if it still works. it's fast enough anyway
    merge_sub_sequences(sub_sequences_by_full_sequence, strict=True)
    merge_same_endpoints(sub_sequences_by_full_sequence, train_data)
    merge_sub_sequences(sub_sequences_by_full_sequence, strict=False)
    print(f"Trains with ref {ref}: mapped {len(train_sequences)} sequences down to {len(sub_sequences_by_full_sequence)} routes")

    route_identifier_by_full_sequence = {sequence: make_train_route_identifier(ref, sequence, train_data) for sequence in sub_sequences_by_full_sequence.keys()}

    route_identifier_by_sequence = {seq: route_identifier_by_full_sequence[full_seq] for full_seq, sub_seqs in sub_sequences_by_full_sequence.items() for seq in sub_seqs}

    return route_identifier_by_sequence

def merge_sub_sequences(sub_sequences_by_full_sequence, strict):
    # go from longest to shortest
    # note that sorted creates a list that will not change as the dict is mutated
    for full_seq, sub_seqs in sorted(sub_sequences_by_full_sequence.items(), reverse=True, key=lambda p: len(p[0])):
        if full_seq not in sub_sequences_by_full_sequence:
            # already merged in a previous iteration of the for loop
            continue

        for other_full_seq, other_sub_seqs in list(sub_sequences_by_full_sequence.items()):
            if len(other_full_seq) >= len(full_seq):
                # can't be a sub-sequence if it's not smaller
                continue
            if is_sub_sequence(other_full_seq, full_seq, strict):
                # remove the other sequence
                del sub_sequences_by_full_sequence[other_full_seq]
                # concatenate lists
                sub_seqs += other_sub_seqs

def is_sub_sequence(short_seq, long_seq, strict):
    # checks whether short_seq is a sub-sequence of long_seq
    # if strict is True, short_seq must appear consecutively in long_seq
    if any(stop not in long_seq for stop in short_seq):
        return False
    if strict:
        i = long_seq.index(short_seq[0])
        j = long_seq.index(short_seq[-1])
        return tuple(long_seq[i:j+1]) == tuple(short_seq)
    else:
        return tuple(short_seq) == tuple(s for s in long_seq if s in short_seq)

def merge_same_endpoints(sub_sequences_by_full_sequence, train_data):
    # note the use of list(sub_sequences_by_full_sequence.items())
    # the dict sub_sequences_by_full_sequence gets mutated inside the loop
    # so we have to make a copy of all we're going to iterate over in advance
    # then at the start of the iteration we check if the key we're using is still valid
    for full_seq, sub_seqs in sorted(sub_sequences_by_full_sequence.items(), reverse=True, key=lambda p: len(p[0])):
        if full_seq not in sub_sequences_by_full_sequence:
            # already merged in a previous iteration of the for loop
            continue

        # compare with all other trips
        for other_full_seq, other_sub_seqs in list(sub_sequences_by_full_sequence.items()):
            if other_sub_seqs == sub_seqs:
                # don't merge with yourself
                # we compare sub_seqs because the same list object stays associated with this sequence even after mergers
                # as opposed to the sequence itself which is a tuple and might change
                continue

            if sequences_have_same_endpoints(full_seq, other_full_seq):
                # remove these two
                del sub_sequences_by_full_sequence[full_seq]
                del sub_sequences_by_full_sequence[other_full_seq]

                # concatenate lists
                sub_seqs += other_sub_seqs

                # create new route which includes all stops in both sequences
                full_seq = merge_sequences(full_seq, other_full_seq, train_data)
                sub_sequences_by_full_sequence[full_seq] = sub_seqs

def sequences_have_same_endpoints(seq1, seq2):
    return seq1[0] == seq2[0] and seq1[-1] == seq2[-1]

def merge_sequences(seq1, seq2, train_data):
    shared_items = set(seq1) & set(seq2)

    assert len(set(seq1)) == len(seq1), "sequence should have no repeats"
    assert len(set(seq2)) == len(seq2), "sequence should have no repeats"
    assert sorted(shared_items, key=lambda v: seq1.index(v)) == sorted(shared_items, key=lambda v: seq2.index(v)), "shared elements should appear in the same order"

    new_seq = []

    # iterate over the sequences...
    i1 = 0
    i2 = 0
    while i1 < len(seq1) and i2 < len(seq2):
        v1 = seq1[i1]
        v2 = seq2[i2]

        if v1 == v2:
            # same element in both lists
            new_seq.append(v1)
            i1 += 1
            i2 += 1
        elif v1 in seq2:
            # seq2 has elements that seq1 doesn't
            j = seq2.index(v1)
            new_seq += seq2[i2:j]
            i2 = j
        elif v2 in seq1:
            # seq1 has elements that seq2 doesn't
            j = seq1.index(v2)
            new_seq += seq1[i1:j]
            i1 = j
        else:
            # find the next shared item between them
            for v in seq1[i1:]:
                if v in shared_items:
                    break
            j1 = seq1.index(v)
            j2 = seq2.index(v)
            new_seq += sort_sub_sequence(seq1[i1:j1] + seq2[i2:j2], train_data)
            i1 = j1
            i2 = j2

    # check our work
    assert new_seq[0] == seq1[0]
    assert new_seq[-1] == seq1[-1]
    assert len(new_seq) == len(set(seq1) | set(seq2))
    assert [v for v in new_seq if v in seq1] == list(seq1) # all stops are there, in the same order
    assert [v for v in new_seq if v in seq2] == list(seq2)

    return tuple(new_seq)

def sort_sub_sequence(stop_ids, train_data):
    # look for a sequence that has all of these stops
    # by all accounts, such a sequence does exist
    for seq in train_data['sequence_by_trip_id'].values():
        if all(s in seq for s in stop_ids):
            # jackpot
            return tuple(s for s in seq if s in stop_ids)

    # need to look at multiple sequences to find the order between all the stops
    # sounds doable but currently not needed
    raise NotImplementedError("Expected the train data to be complete enough for easy coding")

def make_train_route_identifier(ref, sequence, train_data):
    train_stops_by_stop_id = train_data['stops_by_stop_id']
    origin = train_stops_by_stop_id[sequence[0]]['stop_name']
    destination = train_stops_by_stop_id[sequence[-1]]['stop_name']
    return (ref, origin, destination)
### END Train code ###

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--shape", help="geojson file containing the area to analyze")
    group.add_argument("-t", "--trains", action="store_true", help="output all train routes in the country")
    parser.add_argument("-g", "--gtfsdir", required=True, help="directory containing the unzipped GTFS files")
    parser.add_argument("-o", "--outfile", required=True, help="output file, json")
    args = parser.parse_args()
    main(args.shape, args.trains, args.gtfsdir, args.outfile)
