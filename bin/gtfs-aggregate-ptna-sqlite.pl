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
my $ignore_calendar          = undef;
my $language                 = 'de';

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'agency=s'              =>  \$agency,                # --agency=
            'ignore-calendar'       =>  \$ignore_calendar,       # --ignore-calendar
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

my $dbh = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { RaiseError => 1 } ) or die $DBI::errstr;


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

@route_ids_of_agency = FindRouteIdsOfAgency( $agency );

printf STDERR "Routes of agencies selected: %d\n", scalar(@route_ids_of_agency)  if ( $verbose );

@trip_ids_are_valid = FindValidTrips( \@route_ids_of_agency );

printf STDERR "Find  Unique Trip-IDs\n"         if ( $verbose );
CreatePtnaTripsTable();
@unique_trip_ids = FindUniqueTripIds( \@trip_ids_are_valid );
printf STDERR "Found Unique Trip-IDs\n"         if ( $verbose );

printf STDERR "Create new Tables\n"             if ( $verbose );
CreateNewShapesTable();
CreateNewStopTimesTable();
CreateNewTripsTable();
CreateNewRoutesTable();
printf STDERR "New Tables created\n"            if ( $verbose );

printf STDERR "Fill New Shapes Table\n"         if ( $verbose );
FillNewShapesTable( \@unique_trip_ids );
printf STDERR "New Shapes Table filled\n"   if ( $verbose );

printf STDERR "Fill New Stop Times Table\n"     if ( $verbose );
FillNewStopTimesTable( \@unique_trip_ids );
printf STDERR "New Stop Times Table filled\n"   if ( $verbose );

printf STDERR "Fill New Trips Table\n"          if ( $verbose );
FillNewTripsTable(     \@unique_trip_ids );
printf STDERR "New Trips Tables filled\n"       if ( $verbose );

printf STDERR "Fill New Routes Tables\n"        if ( $verbose );
FillNewRoutesTable(    \@unique_trip_ids );
printf STDERR "New Routes Tables filled\n"      if ( $verbose );

StoreImprovements();

printf STDERR "Rename New Tables\n"             if ( $verbose );
RenameAndDropShapesTable();
RenameAndDropStopTimesTable();
RenameAndDropTripsTable();
RenameAndDropRoutesTable();
printf STDERR "New Tables renamed\n"            if ( $verbose );

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

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();
    my @return_array = ();

    if ( $ignore_calendar ) {
        $stmt = sprintf( "SELECT DISTINCT trips.trip_id
                          FROM            trips
                          JOIN            calendar ON trips.service_id = calendar.service_id
                          WHERE           trips.route_id='%s';",
                          $route_id );
    } else {
        my ($sec,$min,$hour,$day,$month,$year) = localtime();

        my $today = sprintf( "%04d%02d%02d", $year+1900, $month+1, $day );

        $stmt = sprintf( "SELECT DISTINCT trips.trip_id
                          FROM            trips
                          JOIN            calendar ON trips.service_id = calendar.service_id
                          WHERE           trips.route_id='%s' AND %s <= calendar.end_date;",
                          $route_id, $today );
    }

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

sub CreatePtnaTripsTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS ptna_trips;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "CREATE TABLE ptna_trips (trip_id TEXT DEFAULT '' PRIMARY KEY UNIQUE, list_trip_ids TEXT DEFAULT '', list_departure_times TEXT DEFAULT '');" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


#############################################################################################
#
#
#

sub FindUniqueTripIds {
    my $array_ref = shift;

    my @ret_array = ();

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    my $stop_id_list_as_string = '';
    my %stop_list_hash         = ();
    my $representative_trip_id = '';
    my $departure_time         = '';

    my %collection_trip_id     = ();

    my $totals    = scalar( @{$array_ref} );
    my $tripcount = 0;
    my $uniques   = 0;

    printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, $uniques, $totals  if ( $verbose );

    foreach my $trip_id ( @trip_ids_are_valid  ) {

        $tripcount++;

        $stop_id_list_as_string = FindStopIdListAsString( $trip_id );

        printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, $uniques, $totals  if ( $verbose );

        if ( !defined($stop_list_hash{$stop_id_list_as_string}) ) {
            $stop_list_hash{$stop_id_list_as_string} = $trip_id;
            $representative_trip_id                  = $trip_id;
            push( @ret_array, $trip_id );
            $uniques++;
            printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, $uniques, $totals  if ( $verbose );
        } else {
            $representative_trip_id = $stop_list_hash{$stop_id_list_as_string};
        }

        $stmt = sprintf( "SELECT   departure_time
                          FROM     stop_times
                          WHERE    trip_id='%s'
                          ORDER BY CAST (stop_sequence AS INTEGER) LIMIT 1;",
                          $trip_id
                       );
        $sth = $dbh->prepare( $stmt );
        $sth->execute();
        @row = $sth->fetchrow_array();
        $departure_time = $row[0] || '-';

        $collection_trip_id{$representative_trip_id}{'similars'}{$trip_id}          = 1;
        $collection_trip_id{$representative_trip_id}{'departures'}{$departure_time} = 1;
    }

    foreach my $trip_id ( keys ( %collection_trip_id ) ) {
        $stmt = sprintf( "INSERT INTO ptna_trips
                          ( trip_id, list_trip_ids, list_departure_times )
                          VALUES ( ?, ?, ? );"
                       );
        $sth   = $dbh->prepare( $stmt );
        $sth->execute( $trip_id,
                       join( '|', sort( keys ( %{$collection_trip_id{$trip_id}{'similars'}} ) ) ),
                       join( '|', sort( keys ( %{$collection_trip_id{$trip_id}{'departures'}} ) ) )
                     );
        printf STDERR "Trip: %06d, Unique: %06d, Total: %06d\r", $tripcount, --$uniques, $totals  if ( $verbose );
    }

    printf STDERR "\n"  if ( $verbose );

    return @ret_array;
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

sub CreateNewShapesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS new_shapes;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "SELECT sql FROM sqlite_master WHERE type='table' AND name='shapes';" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();
    @row  = $sth->fetchrow_array();
    if ( $row[0] ) {
        $row[0] =~ s/CREATE TABLE shapes/CREATE TABLE new_shapes/;
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

sub FillNewShapesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();
    my $has_shape_id = 0;

    $stmt = sprintf( "PRAGMA table_info(trips);" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[1] && $row[1] eq 'shape_id' ) {
            $has_shape_id = 1;
            last;
        }
    }

    if ( $has_shape_id ) {
        foreach my $trip_id ( @{$array_ref} ) {
            $stmt = "INSERT INTO new_shapes SELECT shapes.* FROM shapes JOIN trips ON trips.shape_id = shapes.shape_id WHERE trips.trip_id=?;";
            $sth  = $dbh->prepare( $stmt );
            $sth->execute( $trip_id );
        }
    }

    return 0;
}


#############################################################################################
#
#
#

sub RenameAndDropShapesTable {
    my $array_ref = shift;

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    $stmt = sprintf( "DROP TABLE IF EXISTS shapes;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "ALTER TABLE new_shapes RENAME TO shapes;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
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
    my $array_ref   = shift;
    my $hash_ref    = shift;

    my $stmt            = '';
    my $sth             = undef;
    my @row             = ();
    my %departure_time  = ();

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
