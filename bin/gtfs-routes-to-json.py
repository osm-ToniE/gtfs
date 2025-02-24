#!/usr/bin/env python3

import os.path
import csv
import json
import argparse
import re

def main( gtfs_feed, gtfs_dir, out_file):
    gtfs_routes = get_gtfs_routes(gtfs_dir)
    ptna_routes = convert_to_ptna_routes(gtfs_feed,gtfs_routes)
    sort_routes(ptna_routes)
    output_routes(ptna_routes, out_file)

def get_gtfs_routes(gtfs_dir):
    agency_file = os.path.join(gtfs_dir, 'agency.txt')
    with open(agency_file, newline='', encoding='utf_8') as csvfile:
        reader = csv.DictReader(csvfile)
        agency_name_by_agency_id = {agency['agency_id']: agency['agency_name'] for agency in reader}

    routes_file = os.path.join(gtfs_dir, 'routes.txt')
    with open(routes_file, newline='', encoding='utf_8') as csvfile:
        reader = csv.DictReader(csvfile)
        routes = list(reader)

    for route in routes:
        route['agency_name'] = agency_name_by_agency_id[route['agency_id']]

    return routes

def convert_to_ptna_routes(gtfs_feed,gtfs_routes):
    return [gtfs_route_to_ptna_route(gtfs_feed,route) for route in gtfs_routes]

def gtfs_route_to_ptna_route(gtfs_feed,route):
    # route_id,agency_id,route_short_name,route_long_name,route_type
    return {
        'ref': route['route_short_name'],
        'type': gtfs_route_type_to_csv_type(route['route_type']),
        'comment': route['route_long_name'],
        'operator': route['agency_name'],
        'gtfs_feed': gtfs_feed,
        'route_id': route['route_id'],
    }

def sort_routes(ptna_routes):
    def sort_key(route):
        ref = route['ref']
        ref_num = re.search(r'\d+(\.\d+)?', ref)
        ref_num = float(ref_num.group()) if ref_num else float('-inf')
        return (route['route_type'], ref_num, ref, route['route_id'])
    ptna_routes.sort(key=sort_key)

def output_routes(ptna_routes, out_file):
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(ptna_routes, f, ensure_ascii=False)

def gtfs_route_type_to_csv_type(gtfs_route_type):
    # cover just the route types present in bodo.zip for now
    return {
        '0': 'tram',
        '2': 'train',
        '3': 'bus',
        '4': 'ferry'
    }[gtfs_route_type]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--gtfsfeed", required=True, help="name of the GTFS feed")
    parser.add_argument("-g", "--gtfsdir", required=True, help="directory containing the unzipped GTFS files")
    parser.add_argument("-o", "--outfile", required=True, help="output file, json")
    args = parser.parse_args()
    main(args.gtfsfeed,args.gtfsdir, args.outfile)
