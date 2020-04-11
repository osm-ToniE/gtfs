#!/usr/bin/perl

use warnings;
use strict;

####################################################################################################################
#
#
#
####################################################################################################################

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

my $debug                    = undef;
my $verbose                  = undef;
my $agency                   = undef;

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'agency=s'              =>  \$agency,                # --agency=
          );

if ( $ARGV[0] ) {
    $DB_NAME = $ARGV[0];
}


####################################################################################################################
#
#
#
####################################################################################################################

if ( !(-f $DB_NAME && -w $DB_NAME) ) {
    printf STDERR "Database %s does not exist or can not be written\n", $DB_NAME;
    exit 1;
}

my $dbh = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { RaiseError => 1 } ) or die $DBI::errstr;


####################################################################################################################
#
#
#
####################################################################################################################

my $start_time = time();


my @route_ids_of_agency             = ();
my @route_ids_are_valid             = ();
my @trip_ids_of_route_id_are_valid  = ();
my @trip_ids_are_valid              = ();
my $stop_id_list_as_string          = '';
my %stop_list_hash                  = ();
my @unique_trip_ids                 = ();


@route_ids_of_agency = FindRouteIdsOfAgency( $agency );

printf STDERR "Routes of agencies selected: %d\n", scalar(@route_ids_of_agency)  if ( $verbose );

foreach my $route_id ( @route_ids_of_agency  ) {

    @trip_ids_of_route_id_are_valid  = FindValidTripIdsOfRouteId( $route_id );

    push( @trip_ids_are_valid, @trip_ids_of_route_id_are_valid );

    printf STDERR "Valid trips of route %s = %d\n", $route_id, scalar(@trip_ids_of_route_id_are_valid)  if ( $debug );
}

my $totals    = scalar(@trip_ids_are_valid);
my $tripcount = 0;
my $uniques   = 0;

printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, $uniques, $totals  if ( $verbose );

foreach my $trip_id ( @trip_ids_are_valid  ) {

    $tripcount++;

    $stop_id_list_as_string = FindStopIdListAsString( $trip_id );

    printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, $uniques, $totals  if ( $verbose );

    if ( !defined($stop_list_hash{$stop_id_list_as_string}) ) {
        $stop_list_hash{$stop_id_list_as_string} = $trip_id;
        push( @unique_trip_ids, $trip_id );
        $uniques++;
        printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, $uniques, $totals  if ( $verbose );
    }
}

printf STDERR "\n"  if ( $verbose );

my $size_before = (stat($DB_NAME))[7];

printf STDERR "Create new Tables\n"  if ( $verbose );

CreateNewStopTimesTable();
CreateNewTripsTable();
CreateNewRoutesTable();

printf STDERR "New Tables created\n"  if ( $verbose );

printf STDERR "Fill New Stop Times Table\n"  if ( $verbose );
FillNewStopTimesTable( \@unique_trip_ids );
printf STDERR "New Stop Times Table filled\n"  if ( $verbose );

printf STDERR "Fill New Trips Table\n"  if ( $verbose );
FillNewTripsTable(     \@unique_trip_ids );
printf STDERR "New Trips Tables filled\n"  if ( $verbose );

printf STDERR "Fill New Routes Tables\n"  if ( $verbose );
FillNewRoutesTable(    \@unique_trip_ids );
printf STDERR "New Routes Tables filled\n"  if ( $verbose );

StoreImprovements();

printf STDERR "Rename New Tables\n"  if ( $verbose );
RenameAndDropStopTimesTable();
RenameAndDropTripsTable();
RenameAndDropRoutesTable();
printf STDERR "New Tables renamed\n"  if ( $verbose );

Vacuum();

my $size_after = (stat($DB_NAME))[7];

UpdatePtnaAggregation( time() - $start_time, $size_before, $size_after );

ShowImprovements();

exit 0;


####################################################################################################################
#
#
#
####################################################################################################################

sub FindRouteIdsOfAgency {
    my $agency = shift;

    my $stmt         = '';
    my $sth          = undef;
    my $where_clause = '';
    my @row          = ();
    my @return_array = ();

    if ( $agency ) {
        $where_clause = sprintf( "WHERE agency.agency_id='%s' OR agency.agency_name='%s'", $agency, $agency );
    }

    $stmt = sprintf( "SELECT DISTINCT routes.route_id
                      FROM            routes
                      JOIN            agency ON routes.agency_id = agency.agency_id
                      %s
                      ORDER BY        route_short_name;",
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

sub FindValidTripIdsOfRouteId {
    my $route_id     = shift || '-';

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();
    my @return_array = ();

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d%02d%02d", $year+1900, $month+1, $day );

    $stmt = sprintf( "SELECT DISTINCT trips.trip_id
                      FROM            trips
                      JOIN            calendar ON trips.service_id = calendar.service_id
                      WHERE           trips.route_id='%s' AND %s < calendar.end_date;",
                      $route_id, $today );

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

sub FindStopIdListAsString {
    my $trip_id      = shift || '-';

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "SELECT   GROUP_CONCAT(stop_id,'|')
                      FROM     stop_times
                      WHERE    trip_id='%s'
                      ORDER BY CAST (stop_sequence AS INTEGER);",
                      $trip_id
                   );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

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

sub CreateNewStopTimesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS new_stop_times;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "SELECT sql FROM sqlite_master WHERE type='table' AND name='stop_times';" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();
    @row  = $sth->fetchrow_array();
    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE stop_times/CREATE TABLE new_stop_times/;
        $stmt   = sprintf( "%s;", $row[0] );
        $sth  = $dbh->prepare( $stmt );
        $sth->execute();
    }

    return 0;
}


#############################################################################################
#
#
#

sub FillNewStopTimesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    foreach my $trip_id ( @{$array_ref} ) {
        $stmt = sprintf( "INSERT INTO new_stop_times SELECT * FROM stop_times WHERE stop_times.trip_id='%s';", $trip_id );
        $sth  = $dbh->prepare( $stmt );
        $sth->execute();
    }

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropStopTimesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS stop_times;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "ALTER TABLE new_stop_times RENAME TO stop_times;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


#############################################################################################
#
#
#

sub CreateNewTripsTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS new_trips;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "SELECT sql FROM sqlite_master WHERE type='table' AND name='trips';" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();
    @row  = $sth->fetchrow_array();
    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE trips/CREATE TABLE new_trips/;
        $sth  = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    return 0;
}


#############################################################################################
#
#
#

sub FillNewTripsTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    foreach my $trip_id ( @{$array_ref} ) {
        $stmt = sprintf( "INSERT INTO new_trips SELECT * FROM trips WHERE trips.trip_id='%s';", $trip_id );
        $sth  = $dbh->prepare( $stmt );
        $sth->execute();
    }

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropTripsTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS trips;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "ALTER TABLE new_trips RENAME TO trips;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


#############################################################################################
#
#
#

sub CreateNewRoutesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS new_routes;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "SELECT sql FROM sqlite_master WHERE type='table' AND name='routes';" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();
    @row  = $sth->fetchrow_array();
    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE routes/CREATE TABLE new_routes/;
        $sth  = $dbh->prepare( $row[0] );
        $sth->execute();
    }

    return 0;
}


#############################################################################################
#
#
#

sub FillNewRoutesTable {
    my $array_ref   = shift;

    my $stmt        = '';
    my $sth         = undef;
    my @row         = ();
    my %have_seen   = ();

    foreach my $trip_id ( @{$array_ref} ) {
        $stmt = sprintf( "SELECT DISTINCT route_id FROM trips WHERE trips.trip_id='%s';", $trip_id );
        $sth  = $dbh->prepare( $stmt );
        $sth->execute();

        while ( $row[0] = $sth->fetchrow_array() ) {
            if ( $row[0] && !defined($have_seen{$row[0]}) ) {
                $have_seen{$row[0]} = 1;
                $stmt               = sprintf( "INSERT INTO new_routes SELECT * FROM routes WHERE routes.route_id='%s';", $row[0] );
                $sth                = $dbh->prepare( $stmt );
                $sth->execute();
            }
        }
    }

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropRoutesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS routes;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "ALTER TABLE new_routes RENAME TO routes;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


#############################################################################################
#
#
#

sub StoreImprovements {
    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    my $routes_before       = 0;
    my $routes_after        = 0;
    my $trips_before        = 0;
    my $trips_after         = 0;
    my $stop_times_before   = 0;
    my $stop_times_after    = 0;

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );


    $stmt = sprintf( "DROP TABLE IF EXISTS ptna_aggregation;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "CREATE TABLE ptna_aggregation (
                                   'id'                 INTEGER DEFAULT 0 PRIMARY KEY,
                                   'date'               TEXT,
                                   'duration'           INTEGER DEFAULT 0,
                                   'size_before'        INTEGER DEFAULT 0,
                                   'size_after'         INTEGER DEFAULT 0,
                                   'routes_before'      INTEGER DEFAULT 0,
                                   'routes_after'       INTEGER DEFAULT 0,
                                   'trips_before'       INTEGER DEFAULT 0,
                                   'trips_after'        INTEGER DEFAULT 0,
                                   'stop_times_before'  INTEGER DEFAULT 0,
                                   'stop_times_after'   INTEGER DEFAULT 0
                      );"
                   );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt               = sprintf( "SELECT COUNT(*) FROM routes;" );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $routes_before      = $row[0]   if ( defined($row[0]) );

    $stmt               = sprintf( "SELECT COUNT(*) FROM new_routes;" );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $routes_after       = $row[0]   if ( defined($row[0]) );

    $stmt               = sprintf( "SELECT COUNT(*) FROM trips;" );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $trips_before       = $row[0]   if ( defined($row[0]) );

    $stmt               = sprintf( "SELECT COUNT(*) FROM new_trips;" );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $trips_after        = $row[0]   if ( defined($row[0]) );

    $stmt               = sprintf( "SELECT COUNT(*) FROM stop_times;" );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stop_times_before  = $row[0]   if ( defined($row[0]) );

    $stmt               = sprintf( "SELECT COUNT(*) FROM new_stop_times;" );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stop_times_after   = $row[0]   if ( defined($row[0]) );

    $stmt               = sprintf( "INSERT INTO ptna_aggregation
                                           (id,date,routes_before,routes_after,trips_before,trips_after,stop_times_before,stop_times_after)
                                    VALUES (1, '%s',%d,           %d,          %d,          %d,         %d,               %d              );",
                                    $today, $routes_before, $routes_after, $trips_before, $trips_after, $stop_times_before, $stop_times_after
                                 );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    @row                = $sth->fetchrow_array();
    $stop_times_before  = $row[0]   if ( defined($row[0]) );

    $stmt  = sprintf( "UPDATE ptna SET aggregated='%s' WHERE id=1;", $today );
    $sth   = $dbh->prepare( $stmt );
    $sth->execute();

    return;
}


#############################################################################################
#
#
#

sub UpdatePtnaAggregation {
    my $seconds    = shift || 0;
    my $sizebefore = shift || 0;
    my $sizeafter  = shift || 0;

    my $stmt    = '';
    my $sth     = undef;
    my @row     = ();

    $stmt = sprintf( "UPDATE ptna_aggregation SET duration=%d,size_before=%d,size_after=%d WHERE id=1;", $seconds, $sizebefore, $sizeafter );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


#############################################################################################
#
#
#

sub ShowImprovements {

    my $stmt    = '';
    my $sth     = undef;
    my @row     = ();

    return 0;
}


#############################################################################################
#
#
#

sub Vacuum {

    my $stmt    = '';
    my $sth     = undef;
    my @row     = ();

    $stmt = sprintf( "VACUUM;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}

