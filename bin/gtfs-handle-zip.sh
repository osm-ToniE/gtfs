#!/bin/bash

#
# the GTFS-zip file should be store in a location like this:
#
# $PWD ends like this ... gtfs-network/DE/BY/MVV/2020-03-17"
#
# where DE-BY-MVV later on build the 'network' part of the target DB file
# where DE and BY and MVV later on build part of the target path /osm/ptna/work/DE/BY/DE-BY-MVV-ptna-gtfs-sqlite.db
# where we expect two files from former analysis to be in the parent directory, i.e.
# ../osm.txt
# ../ptna.txt
#

ANALYSIS_LANG="--language=de"

echo $(date '+%Y-%m-%d %H:%M:%S') "start preparation $*"
gtfs-prepare-ptna-sqlite.sh $*

echo $(date '+%Y-%m-%d %H:%M:%S') "start aggregation $*"
gtfs-aggregate-ptna-sqlite.pl $*

echo $(date '+%Y-%m-%d %H:%M:%S') "start analysis $ANALYSIS_LANG $*"
gtfs-analyze-ptna-sqlite.pl $ANALYSIS_LANG $*

echo $(date '+%Y-%m-%d %H:%M:%S') "start normalization $ANALYSIS_LANG $*"
gtfs-normalize-ptna-sqlite.pl $ANALYSIS_LANG $*
