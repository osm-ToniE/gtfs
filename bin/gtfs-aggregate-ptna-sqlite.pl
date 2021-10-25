#!/usr/bin/perl

use warnings;
use strict;

####################################################################################################################
#
#
#

use POSIX;

use utf8;
binmode STDIN,  ":utf8";
binmode STDOUT, ":utf8";
binmode STDERR, ":utf8";
use Encode;

use DBI;


#############################################################################################
#
#
#

my $DB_NAME = "ptna-gtfs-sqlite.db";


#############################################################################################
#
#
#

use Getopt::Long;

my $debug                    = 0;
my $verbose                  = 0;
my $agency                   = undef;
my $consider_calendar        = undef;
my $language                 = 'de';

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'agency=s'              =>  \$agency,                # --agency=
            'consider-calendar'     =>  \$consider_calendar,     # --consider-calendar
            'language=s'            =>  \$language,              # --language=de
          );

if ( $ARGV[0] ) {
    $DB_NAME = $ARGV[0];
}


####################################################################################################################
#
#
#

if ( !(-f $DB_NAME && -w $DB_NAME) ) {
    printf STDERR "Database %s does not exist or can not be written\n", $DB_NAME;
    exit 1;
}

my $dbh      = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { AutoCommit => 0, RaiseError => 1 } ) or die $DBI::errstr;

my $dbh_temp = DBI->connect( "DBI:SQLite:dbname=:memory:", "", "", { AutoCommit => 0, RaiseError => 1 } ) or die $DBI::errstr;


####################################################################################################################
#
#
#

my $start_time  = time();
my $size_before = (stat($DB_NAME))[7];

my @route_ids_of_agency             = ();
my @trip_ids_are_valid              = ();
my @unique_trip_ids                 = ();


####################################################################################################################
#
#
#

printf STDERR "%s Find  Route-IDs of agency out of %d total\n", get_time(), CountAllRoutes();
@route_ids_of_agency = FindRouteIdsOfAgency( $agency );
printf STDERR "%s Found Route-IDs of agency: %d\n", get_time(), scalar(@route_ids_of_agency);

printf STDERR "%s Find  valid trips out of %d total\n", get_time(), CountAllTrips();
@trip_ids_are_valid = FindValidTrips( \@route_ids_of_agency );
printf STDERR "%s Found valid trips: %d\n", get_time(), scalar(@trip_ids_are_valid);

printf STDERR "%s Find  Unique Trip-IDs\n", get_time();
CreatePtnaTripsTable();
@unique_trip_ids = FindUniqueTripIds( \@trip_ids_are_valid );
printf STDERR "%s Found Unique Trip-IDs: %d \n", get_time(), scalar(@unique_trip_ids);

printf STDERR "%s Create new Tables\n", get_time();
CreateNewShapesTable();
CreateNewStopTimesTable();
CreateNewStopsTable();
CreateNewTripsTable();
CreateNewRoutesTable();
printf STDERR "%s New Tables created\n", get_time();

printf STDERR "%s Fill New Shapes Table\n", get_time();
FillNewShapesTable( \@unique_trip_ids );
printf STDERR "%s New Shapes Table filled\n", get_time();

printf STDERR "%s Fill New Stop Times Table\n", get_time();
FillNewStopTimesTable( \@unique_trip_ids );
printf STDERR "%s New Stop Times Table filled\n", get_time();

printf STDERR "%s Fill New Stops Table\n", get_time();
FillNewStopsTable();        # no parameter, will read stop_ids from from new_stop_times to findvalid stop_ids
printf STDERR "%s New Stops Table filled\n", get_time();

printf STDERR "%s Fill New Trips Table\n", get_time();
FillNewTripsTable(     \@unique_trip_ids );
printf STDERR "%s New Trips Tables filled\n", get_time();

printf STDERR "%s Fill New Routes Tables\n", get_time();
FillNewRoutesTable(    \@unique_trip_ids );
printf STDERR "%s New Routes Tables filled\n", get_time();

StoreImprovements();

printf STDERR "%s Rename New Tables\n", get_time();
RenameAndDropShapesTable();
RenameAndDropStopsTable();
RenameAndDropStopTimesTable();
RenameAndDropTripsTable();
RenameAndDropRoutesTable();
printf STDERR "%s New Tables renamed\n", get_time();

printf STDERR "%s Vacuum\n", get_time();
Vacuum();
printf STDERR "%s Vacuum done\n", get_time();

my $size_after = (stat($DB_NAME))[7];

printf STDERR "%s Update Ptna Aggregation\n", get_time();
UpdatePtnaAggregation( time() - $start_time, $size_before, $size_after );
printf STDERR "%s Update Ptna Aggregation done\n", get_time();

ShowImprovements();

exit 0;


#############################################################################################
#
#
#

sub CountAllRoutes {

    my $sth = $dbh->prepare( "SELECT COUNT(route_id) FROM routes;" );
    my @row = ();

    $sth->execute();

    @row = $sth->fetchrow_array();

    return $row[0] || 0;
}


####################################################################################################################
#
#
#
####################################################################################################################

sub FindRouteIdsOfAgency {
    my $agency       = shift;

    my $stmt         = '';
    my $sth          = undef;
    my $join_clause  = '';
    my $where_clause = '';
    my @row          = ();
    my @return_array = ();

    if ( $agency ) {
        $join_clause  = "JOIN agency ON routes.agency_id = agency.agency_id";
        my @agencies = split( ',', $agency );
        $where_clause = 'WHERE';
        foreach $agency ( @agencies ) {
            $where_clause .= sprintf( " agency.agency_id='%s' OR agency.agency_name='%s' OR", $agency, $agency );
        }
        $where_clause =~ s/ OR$//;
    }

    $stmt = sprintf( "SELECT DISTINCT routes.route_id
                      FROM            routes
                      %s
                      %s
                      ORDER BY        route_short_name;",
                      $join_clause,
                      $where_clause
                   );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            push( @return_array, $row[0] );
        }
    }

    return @return_array;
}


#############################################################################################
#
#
#

sub CountAllTrips {

    my $sth = $dbh->prepare( "SELECT COUNT(trip_id) FROM trips;" );
    my @row = ();

    $sth->execute();

    @row = $sth->fetchrow_array();

    return $row[0] || 0;
}


#############################################################################################
#
#
#

sub FindValidTrips {
    my $array_ref = shift;
    my @ret_array = ();

    foreach my $route_id ( @{$array_ref}  ) {
        push( @ret_array, FindValidTripIdsOfRouteId( $route_id ) );
    }

    return @ret_array;
}


#############################################################################################
#
#
#

sub FindValidTripIdsOfRouteId {
    my $route_id     = shift || '-';

    my $sth          = undef;
    my @row          = ();
    my @return_array = ();

    if ( $consider_calendar ) {
        my ($sec,$min,$hour,$day,$month,$year) = localtime();

        my $today = sprintf( "%04d%02d%02d", $year+1900, $month+1, $day );

        $sth = $dbh->prepare( "SELECT DISTINCT trips.trip_id
                               FROM            trips
                               JOIN            calendar ON trips.service_id = calendar.service_id
                               WHERE           trips.route_id=? AND ? <= calendar.end_date;"
                            );
        $sth->execute( $route_id, $today );
    } else {
        $sth = $dbh->prepare( "SELECT DISTINCT trip_id
                               FROM            trips
                               WHERE           route_id=?;"
                            );
        $sth->execute( $route_id );
    }

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            push( @return_array, $row[0] );
        }
    }

    return @return_array;
}


#############################################################################################
#
#
#

sub CreatePtnaTripsTable {

    my $sth = undef;

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS ptna_trips;" );
    $sth->execute();

    $sth = $dbh->prepare( "CREATE TABLE ptna_trips (trip_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, list_trip_ids TEXT DEFAULT '', list_departure_times TEXT DEFAULT '', list_durations TEXT DEFAULT '', list_service_ids TEXT DEFAULT '');" );
    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub FindUniqueTripIds {
    my $array_ref = shift;

    my @ret_array = ();

    my $sthR         = undef;
    my $sthD         = undef;
    my $sthA         = undef;
    my $sthI         = undef;
    my @row          = ();

    my $stop_id_list_as_string = '';
    my %stop_list_hash         = ();
    my $representative_trip_id = '';
    my $departure_time         = '';
    my $arrival_time           = '';
    my $departure_secs         = 0;
    my $arrival_secs           = 0;
    my $duration               = '';
    my $duration_secs          = 0;
    my $route_id               = '-';
    my $service_id             = '-';

    my %collection_trip_id     = ();

    my $totals    = scalar( @{$array_ref} );
    my $tripcount = 0;
    my $uniques   = 0;

    printf STDERR "Trip: %06d, Unique: %06d, Stored: %06d, Total: %06d\r", $tripcount, $uniques, 0, $totals  if ( $verbose );

    $sthR = $dbh->prepare( "SELECT   route_id,service_id
                            FROM     trips
                            WHERE    trip_id=?;" );

    $sthD = $dbh->prepare( "SELECT   departure_time
                            FROM     stop_times
                            WHERE    trip_id=?
                            ORDER BY CAST (stop_sequence AS INTEGER) ASC  LIMIT 1;" );

    $sthA = $dbh->prepare( "SELECT   arrival_time
                            FROM     stop_times
                            WHERE    trip_id=?
                            ORDER BY CAST (stop_sequence AS INTEGER) DESC LIMIT 1;" );

    foreach my $trip_id ( @trip_ids_are_valid  ) {

        $tripcount++;

        $sthR->execute( $trip_id );

        @row        = $sthR->fetchrow_array();
        $route_id   = $row[0] || '-';
        $service_id = $row[1] || '-';

        $stop_id_list_as_string = FindStopIdListAsString( $trip_id );

        printf STDERR "Trip: %06d, Unique: %06d, Stored: %06d, Total: %06d\r", $tripcount, $uniques, 0, $totals  if ( $verbose );

        if ( !defined($stop_list_hash{$route_id}{$stop_id_list_as_string}) ) {
            $stop_list_hash{$route_id}{$stop_id_list_as_string} = $trip_id;
            $representative_trip_id                             = $trip_id;
            push( @ret_array, $trip_id );

            printf STDERR "Trip: %06d, Unique: %06d, Stored: %06d, Total: %06d\r", $tripcount, ++$uniques, 0, $totals  if ( $verbose );
        } else {
            $representative_trip_id = $stop_list_hash{$route_id}{$stop_id_list_as_string};
        }

        $sthD->execute( $trip_id );

        @row = $sthD->fetchrow_array();
        $departure_time = $row[0] || '00:00:00';

        $sthA->execute( $trip_id );

        @row = $sthA->fetchrow_array();
        $arrival_time = $row[0] || '00:00:00';

        $duration = '?:??';
        if ( $departure_time =~ m/^(\d{1,2}):(\d\d):(\d\d)$/ ) {
            $departure_secs = $1 * 3600 + $2 * 60 + $3;
            $departure_time = sprintf( "%02d:%02d:%02d", $1, $2, $3 );
            if ( $arrival_time =~ m/^(\d{1,2}):(\d\d):(\d\d)$/ ) {
                $arrival_secs  = $1 * 3600 + $2 * 60 + $3;
                $arrival_time  = sprintf( "%02d:%02d:%02d", $1, $2, $3 );
                $duration_secs = $arrival_secs - $departure_secs;
                $duration = sprintf( "%d:%02d:%02d", $duration_secs / 3600, ($duration_secs % 3600) / 60, $duration_secs % 60 );
            }
        }

        push( @{$collection_trip_id{$representative_trip_id}{'similars'}},   $trip_id        );
        push( @{$collection_trip_id{$representative_trip_id}{'departures'}}, $departure_time );
        push( @{$collection_trip_id{$representative_trip_id}{'durations'}},  $duration       );
        push( @{$collection_trip_id{$representative_trip_id}{'service_id'}}, $service_id     );
    }

    printf STDERR "\n"                                          if ( $verbose );
    printf STDERR "%s Start Storing ptna_trips ...\n", get_time();

    $sthI = $dbh->prepare( "INSERT INTO ptna_trips
                                   ( trip_id, list_trip_ids, list_departure_times, list_durations, list_service_ids )
                            VALUES ( ?, ?, ?, ?, ? );" );

    my @new_ret_array = ();
    my $best_trip_id  = '';
    my $stored = 0;
    my %have_seen_trip_id = ();
    foreach my $trip_id ( @ret_array ) {
        $best_trip_id = GetTripIdWithBestServiceInterval( @{$collection_trip_id{$trip_id}{'similars'}} );
        unless ( $have_seen_trip_id{$best_trip_id} ) {
            push( @new_ret_array, $best_trip_id );
            $sthI->execute( $best_trip_id,
                            join( '|', @{$collection_trip_id{$trip_id}{'similars'}}   ),
                            join( '|', @{$collection_trip_id{$trip_id}{'departures'}} ),
                            join( '|', @{$collection_trip_id{$trip_id}{'durations'}}  ),
                            join( '|', @{$collection_trip_id{$trip_id}{'service_id'}} )
                        );
            printf STDERR "Trip: %06d, Unique: %06d, Stored: %06d, Total: %06d\r", $tripcount, $uniques, ++$stored, $totals  if ( $verbose );
        }
        $have_seen_trip_id{$best_trip_id} = 1;
    }

    printf STDERR "\n"  if ( $verbose );

    $dbh->commit();

    return @new_ret_array;
}


#############################################################################################
#
#
#

sub FindStopIdListAsString {
    my $trip_id      = shift || '-';

    my $sth          = undef;
    my @row          = ();

    $sth = $dbh->prepare( "SELECT   GROUP_CONCAT(stop_id,'|')
                           FROM     stop_times
                           WHERE    trip_id=?
                           ORDER BY CAST (stop_sequence AS INTEGER);" );
    $sth->execute( $trip_id );

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            return $row[0];
        }
    }

    return '';

}


#############################################################################################
#
#
#

sub GetTripIdWithBestServiceInterval {
    my @trip_id_array   = @_;

    my $ret_val         = '';

    my @work_array      = ();
    my $original_size   = scalar(@trip_id_array);
    my $remaining       = 0;
    my $best_start_date = 99991231;
    my $best_end_date   = 19700101;

    while ( scalar(@trip_id_array) ) {

        if ( scalar(@trip_id_array) > 900 ) {
            #printf STDERR "GetTripIdWithBestServiceInterval( 0 => %s, ..., 899 => %s, 900 => %s, 901 => %s ): many trip_ids %d, limiting to 900\n", $trip_id_array[0], $trip_id_array[899], $trip_id_array[900], $trip_id_array[901], scalar(@trip_id_array);
            @work_array     = splice( @trip_id_array, 0, 900 );
            $remaining      = scalar(@trip_id_array) - 900;
            @trip_id_array  = splice( @trip_id_array, 0, -$remaining );
        } else {
            #printf STDERR "GetTripIdWithBestServiceInterval( 0 => %s, ..., %d => %s ): trip_ids %d\n", $trip_id_array[0], $#trip_id_array,  $trip_id_array[$#trip_id_array], scalar(@trip_id_array);
            @work_array     = @trip_id_array;
            @trip_id_array  = ();
            $remaining      = 0;
        }

        my $sth = undef;
        my @row = ();
        my $where_clause = join( '', map{'? OR trip_id='} @work_array );
        $where_clause =~ s/ OR trip_id=$//;
        my $sql = sprintf( "SELECT trips.trip_id, calendar.start_date, calendar.end_date
                            FROM   trips
                            JOIN   calendar ON trips.service_id = calendar.service_id
                            WHERE  trip_id=%s
                            ORDER  BY calendar.end_date DESC, calendar.start_date ASC LIMIT 1;", $where_clause );
        #printf STDERR "%s\n", $sql;
        $sth = $dbh->prepare( $sql );
        $sth->execute( @work_array );

        while ( @row = $sth->fetchrow_array() ) {
            if ( $row[0] ) {
                #printf STDERR "??? trip_id = %s: start_date = %s, end_date = %s, out of = %s\n", $row[0], $row[1], $row[2], scalar(@work_array) if ( $original_size > 900 );
                if ( $row[1] < $best_start_date && $row[2] > $best_end_date ) {
                    $ret_val         = $row[0];
                    $best_start_date = $row[1];
                    $best_end_date   = $row[2]
                }
            }
        }
    }

    #printf STDERR "--> trip_id = %s: start_date = %s, end_date = %s\n", $ret_val, $best_start_date, $best_end_date  if ( $original_size > 900 );
    return $ret_val;

}


#############################################################################################
#
#
#

sub CreateNewShapesTable {

    my $sth = undef;
    my @row = ();

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS new_shapes;" );
    $sth->execute();

    $sth = $dbh->prepare( "SELECT sql FROM sqlite_master WHERE type='table' AND name='shapes';" );
    $sth->execute();

    @row  = $sth->fetchrow_array();

    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE shapes/CREATE TABLE new_shapes/;
        $sth  = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub FillNewShapesTable {
    my $array_ref = shift;

    my $sth          = undef;
    my @row          = ();
    my $has_shape_id = 0;

    $sth = $dbh->prepare( "PRAGMA table_info(trips);" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[1] && $row[1] eq 'shape_id' ) {
            $has_shape_id = 1;
            last;
        }
    }

    if ( $has_shape_id ) {
        $sth = $dbh->prepare( "INSERT INTO new_shapes SELECT shapes.* FROM shapes JOIN trips ON trips.shape_id = shapes.shape_id WHERE trips.trip_id=?;" );

        foreach my $trip_id ( @{$array_ref} ) {
            $sth->execute( $trip_id );
        }
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropShapesTable {

    my $sth = undef;

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS shapes;" );
    $sth->execute();

    $sth = $dbh->prepare( "ALTER TABLE new_shapes RENAME TO shapes;" );
    $sth->execute();

    $sth = $dbh->prepare( "CREATE INDEX idx_shape_id ON  shapes (shape_id);" );
    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub CreateNewStopsTable {

    my $sth = undef;
    my @row = ();

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS new_stops;" );
    $sth->execute();

    $sth = $dbh->prepare( "SELECT sql FROM sqlite_master WHERE type='table' AND name='stops';" );
    $sth->execute();

    @row = $sth->fetchrow_array();

    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE stops/CREATE TABLE new_stops/;
        $sth    = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub FillNewStopsTable {

    my $sth       = $dbh->prepare( "INSERT INTO new_stops SELECT * FROM stops WHERE stop_id IN ( SELECT DISTINCT stop_id FROM new_stop_times );" );

    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropStopsTable {

    my $sth = undef;

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS stops;" );
    $sth->execute();

    $sth = $dbh->prepare( "ALTER TABLE new_stops RENAME TO stops;" );
    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub CreateNewStopTimesTable {

    my $sth = undef;
    my @row = ();

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS new_stop_times;" );
    $sth->execute();

    $sth = $dbh->prepare( "SELECT sql FROM sqlite_master WHERE type='table' AND name='stop_times';" );
    $sth->execute();

    @row = $sth->fetchrow_array();

    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE stop_times/CREATE TABLE new_stop_times/;
        $sth    = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub FillNewStopTimesTable {
    my $array_ref = shift;

    my $sth       = $dbh->prepare( "INSERT INTO new_stop_times SELECT * FROM stop_times WHERE stop_times.trip_id=?;" );

    foreach my $trip_id ( @{$array_ref} ) {
        $sth->execute( $trip_id );
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropStopTimesTable {

    my $sth = undef;

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS stop_times;" );
    $sth->execute();

    $sth = $dbh->prepare( "ALTER TABLE new_stop_times RENAME TO stop_times;" );
    $sth->execute();

    $sth = $dbh->prepare( "CREATE INDEX idx_trip_id ON  stop_times (trip_id);" );
    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub CreateNewTripsTable {

    my $sth  = undef;
    my @row  = ();

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS new_trips;" );
    $sth->execute();

    $sth = $dbh->prepare( "SELECT sql FROM sqlite_master WHERE type='table' AND name='trips';" );
    $sth->execute();

    @row = $sth->fetchrow_array();

    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE trips/CREATE TABLE new_trips/;
        $sth    = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub FillNewTripsTable {
    my $array_ref   = shift;

    my $sth         = $dbh->prepare( "INSERT INTO new_trips SELECT * FROM trips WHERE trips.trip_id=?;" );

    foreach my $trip_id ( @{$array_ref} ) {
        $sth->execute( $trip_id );
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropTripsTable {

    my $sth = undef;

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS trips;" );
    $sth->execute();

    $sth = $dbh->prepare( "ALTER TABLE new_trips RENAME TO trips;" );
    $sth->execute();

    $sth = $dbh->prepare( "CREATE INDEX idx_route_id ON  trips (route_id);" );
    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub CreateNewRoutesTable {

    my $sth = undef;
    my @row = ();

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS new_routes;" );
    $sth->execute();

    $sth = $dbh->prepare( "SELECT sql FROM sqlite_master WHERE type='table' AND name='routes';" );
    $sth->execute();

    @row = $sth->fetchrow_array();

    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE routes/CREATE TABLE new_routes/;
        $sth    = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub FillNewRoutesTable {
    my $array_ref   = shift;

    my $sthS        = $dbh->prepare( "SELECT DISTINCT route_id FROM trips WHERE trips.trip_id=?;" );
    my $sthI        = $dbh->prepare( "INSERT INTO new_routes SELECT * FROM routes WHERE routes.route_id=?;" );
    my @row         = ();
    my %have_seen   = ();

    foreach my $trip_id ( @{$array_ref} ) {
        $sthS->execute( $trip_id );

        while ( $row[0] = $sthS->fetchrow_array() ) {
            if ( $row[0] && !defined($have_seen{$row[0]}) ) {
                $have_seen{$row[0]} = 1;
                $sthI->execute( $row[0] );
            }
        }
    }

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropRoutesTable {

    my $sth  = undef;

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS routes;" );
    $sth->execute();

    $sth = $dbh->prepare( "ALTER TABLE new_routes RENAME TO routes;" );
    $sth->execute();

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub StoreImprovements {

    my $stmt                = '';
    my $sth                 = undef;
    my @row                 = ();
    my $routes_before       = 0;
    my $routes_after        = 0;
    my $trips_before        = 0;
    my $trips_after         = 0;
    my $stops_before        = 0;
    my $stops_after         = 0;
    my $stop_times_before   = 0;
    my $stop_times_after    = 0;
    my $shapes_before       = 0;
    my $shapes_after        = 0;

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );

    $sth  = $dbh->prepare( "DROP TABLE IF EXISTS ptna_aggregation;" );
    $sth->execute();

    $sth  = $dbh->prepare( "CREATE TABLE ptna_aggregation (
                                   'id'                 INTEGER DEFAULT 0 PRIMARY KEY,
                                   'date'               TEXT,
                                   'duration'           INTEGER DEFAULT 0,
                                   'size_before'        INTEGER DEFAULT 0,
                                   'size_after'         INTEGER DEFAULT 0,
                                   'routes_before'      INTEGER DEFAULT 0,
                                   'routes_after'       INTEGER DEFAULT 0,
                                   'trips_before'       INTEGER DEFAULT 0,
                                   'trips_after'        INTEGER DEFAULT 0,
                                   'stops_before'       INTEGER DEFAULT 0,
                                   'stops_after'        INTEGER DEFAULT 0,
                                   'stop_times_before'  INTEGER DEFAULT 0,
                                   'stop_times_after'   INTEGER DEFAULT 0,
                                   'shapes_before'      INTEGER DEFAULT 0,
                                   'shapes_after'       INTEGER DEFAULT 0
                            );" );
    $sth->execute();

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM routes;"  );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $routes_before      = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM new_routes;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $routes_after       = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM trips;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $trips_before       = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM new_trips;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $trips_after        = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM stops;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stops_before       = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM new_stops;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stops_after        = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM stop_times;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stop_times_before  = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM new_stop_times;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stop_times_after   = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM shapes;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $shapes_before      = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "SELECT COUNT(*) FROM new_shapes;" );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $shapes_after       = $row[0]   if ( defined($row[0]) );

    $sth                = $dbh->prepare( "INSERT INTO ptna_aggregation
                                                 (id,date,routes_before,routes_after,trips_before,trips_after,stops_before,stops_after,stop_times_before,stop_times_after,shapes_before,shapes_after)
                                          VALUES (1, ?,   ?,            ?,           ?,           ?,          ?,           ?,          ?,                ?,               ?,            ?           );" );
    $sth->execute( $today, $routes_before, $routes_after, $trips_before, $trips_after, $stops_before, $stops_after, $stop_times_before, $stop_times_after, $shapes_before, $shapes_after );

    $sth                = $dbh->prepare( "UPDATE ptna SET aggregated=? WHERE id=1;" );
    $sth->execute( $today );

    $dbh->commit();

    return;
}


#############################################################################################
#
#
#

sub UpdatePtnaAggregation {
    my $seconds     = shift || 0;
    my $sizebefore  = shift || 0;
    my $sizeafter   = shift || 0;

    $dbh->{AutoCommit} = 0;

    my $sth         = $dbh->prepare( "UPDATE ptna_aggregation SET duration=?,size_before=?,size_after=? WHERE id=1;" );

    $sth->execute( $seconds, $sizebefore, $sizeafter );

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub ShowImprovements {

    return 0;
}


#############################################################################################
#
#
#

sub Vacuum {

    my $sth = $dbh->prepare( "VACUUM;" );

    $dbh->{AutoCommit} = 1;

    $sth->execute();

    return 0;
}

#############################################################################################
#
#
#

sub get_time {

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    return sprintf( "%04d-%02d-%02d %02d:%02d:%02d", $year+1900, $month+1, $day, $hour, $min, $sec );
}
