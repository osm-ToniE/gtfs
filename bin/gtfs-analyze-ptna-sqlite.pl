#!/usr/bin/perl

use warnings;
use strict;

####################################################################################################################
#
#
#

use POSIX;

use utf8;
binmode STDIN,  ":encoding(UTF-8)";
binmode STDOUT, ":encoding(UTF-8)";
binmode STDERR, ":encoding(UTF-8)";
use Encode;

use DBI;

use Date::Calc qw( Delta_Days );


#############################################################################################
#
#
#

my %route_type_may_have_only_two_stops = (
                                            '4'    => 'Ferry',
                                            '5'    => 'Cable tram',
                                            '6'    => 'Aerialway',
                                            '7'    => 'Funicular',
                                            '108'  => 'Rail Shuttle (Within Complex)',
                                            '711'  => 'Shuttle Bus',
                                            '907'  => 'Aerial Lift Service',
                                            '1000' => 'Water Transport Service',
                                            '1100' => 'Air Service',
                                            '1200' => 'Ferry Service',
                                            '1300' => 'Aerial Lift Service',
                                            '1400' => 'Funicular Service',
                                            '1502' => 'Water Taxi Service'
                                         );

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
my $list_separator           = '|';

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

my $dbh = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { AutoCommit => 0, RaiseError => 1 } ) or die $DBI::errstr;


####################################################################################################################
#
#
#

my ($sec,$min,$hour,$day,$month,$year) = localtime();

my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );


printf STDERR "%s Get list separator\n", get_time();
$list_separator = GetListSeparator();
printf STDERR "%s List separator: %s\n", get_time(), $list_separator;

printf STDERR "%s CreatePtnaAnalysis\n", get_time();
CreatePtnaAnalysis();

printf STDERR "%s ClearAllPtnaCommentsForTrips\n", get_time();
ClearAllPtnaCommentsForTrips();

my $start_time                  = time();

printf STDERR "%s FindRouteIdsOfAgency\n", get_time();
my @route_ids_of_agency         = FindRouteIdsOfAgency( $agency );

my @trip_ids_of_route_id        = ();

my %detail_hash_of_trips        = ();

my $shape_id                    = '';

my $stop_id_list                = '';

my $stop_name_list              = '';

printf STDERR "Routes of agencies selected: %d\n", scalar(@route_ids_of_agency)  if ( $debug );

printf STDERR "%s Mark details of trips\n", get_time();
foreach my $route_id ( @route_ids_of_agency ) {

    @trip_ids_of_route_id       = FindTripIdsOfRouteId( $route_id );

    %detail_hash_of_trips       = ();

    foreach my $trip_id ( @trip_ids_of_route_id ) {

        MarkSuspiciousStart( $trip_id );

        MarkSuspiciousEnd( $trip_id );

        MarkSuspiciousNumberOfStops( $trip_id );

        MarkSuspiciousTripDuration( $trip_id );

        $shape_id        = FindShapeIdOfTripId( $trip_id ) || '-';

        $stop_id_list    = FindStopIdListAsString( $trip_id );

        $stop_name_list  = FindStopNameListAsString( $trip_id );

        $detail_hash_of_trips{$trip_id}{'shape_id'}   = $shape_id;
        $detail_hash_of_trips{$trip_id}{'stop_ids'}   = $stop_id_list;
        $detail_hash_of_trips{$trip_id}{'stop_names'} = $stop_name_list;
    }

    MarkTripsOfRouteId( $route_id, \%detail_hash_of_trips );
}
printf STDERR "%s Mark details of trips ... done\n", get_time();

printf STDERR "%s Calculate Rides ... be patient\n", get_time();
FindNumberOfRidesForTripIds();
CalculateSumRidesOfLongestTrip();
printf STDERR "%s Calculate Rides ... done\n", get_time();

UpdatePtnaAnalysis( time() - $start_time );

exit 0;


#############################################################################################
#
#
#

sub GetListSeparator {

    my $sth = $dbh->prepare( "SELECT * FROM ptna LIMIT 1;" );
       $sth->execute();
    my $hash_ref = $sth->fetchrow_hashref();
    if ( exists($hash_ref->{'list_separator'}) and $hash_ref->{'list_separator'} ) {
        return $hash_ref->{'list_separator'};
    } else {
        return $list_separator;
    }
}


####################################################################################################################
#
#
#

sub FindRouteIdsOfAgency {
    my $agency       = shift;

    my $sth          = undef;
    my @row          = ();
    my @return_array = ();

    if ( $agency ) {
        $sth = $dbh->prepare( "SELECT DISTINCT routes.route_id
                               FROM            routes
                               JOIN            agency ON routes.agency_id = agency.agency_id
                               WHERE           agency.agency_id=? OR agency.agency_name=?
                               ORDER BY        route_short_name;" );
        $sth->execute( $agency, $agency );
    } else {
        $sth = $dbh->prepare( "SELECT DISTINCT routes.route_id
                               FROM            routes
                               JOIN            agency ON routes.agency_id = agency.agency_id
                               ORDER BY        route_short_name;" );
        $sth->execute();
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

sub FindTripIdsOfRouteId {
    my $route_id     = shift || '-';

    my $sth          = undef;
    my @row          = ();
    my @return_array = ();

    $sth = $dbh->prepare( "SELECT DISTINCT trips.trip_id
                           FROM            trips
                           WHERE           trips.route_id=?;" );
    $sth->execute( $route_id );

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

sub FindShapeIdOfTripId {
    my $trip_id     = shift || '-';

    my $sth          = undef;
    my $hash_ref     = undef;

    $sth = $dbh->prepare( "SELECT *
                           FROM   trips
                           WHERE  trip_id=?;" );
    $sth->execute( $trip_id );

    while ( $hash_ref = $sth->fetchrow_hashref() ) {
        if ( $hash_ref->{'shape_id'} ) {
            return $hash_ref->{'shape_id'};
        }
    }

    return '';
}


#############################################################################################
#
# list has already been ordered by stop_sequence in gtfs-aggregate.pl, so group_concat works as expected
#

sub FindStopIdListAsString {
    my $trip_id  = shift || '-';

    my $sth      = undef;
    my @row      = ();

    $sth = $dbh->prepare( "SELECT   GROUP_CONCAT(stop_id,'$list_separator')
                           FROM     stop_times
                           WHERE    trip_id=?
                           ORDER BY CAST (stop_sequence AS INTEGER);" );
    $sth->execute( $trip_id );

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0] ) {
            return $list_separator . $row[0] . $list_separator;
        }
    }

    return '';

}


#############################################################################################
#
# list has already been ordered by stop_sequence in gtfs-aggregate.pl, so group_concat works as expected
#

sub FindStopNameListAsString {
    my $trip_id  = shift || '-';

    my $sth      = undef;
    my @row      = ();

    $sth = $dbh->prepare( "SELECT   GROUP_CONCAT(stops.stop_name,'$list_separator')
                           FROM     stop_times
                           JOIN     stops ON stop_times.stop_id = stops.stop_id
                           WHERE    trip_id=?
                           ORDER BY CAST (stop_sequence AS INTEGER);" );
    $sth->execute( $trip_id );

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0] ) {
            return $list_separator . $row[0] . $list_separator;
        }
    }

    return '';

}


#############################################################################################
#
# check for a suspicious start of this route. I.e. does the bus, ...
# make a u-turn before actually starting the journey with/without passengers?
#
#

sub MarkSuspiciousStart {
    my $trip_id  = shift || '-';

    my $sth      = undef;
    my @row      = ();

    # we are only interested in the first two stops of this trip

    $sth = $dbh->prepare( "SELECT   stop_times.stop_id,stops.stop_name
                           FROM     stop_times
                           JOIN     stops ON stop_times.stop_id = stops.stop_id
                           WHERE    trip_id=?
                           ORDER BY CAST (stop_times.stop_sequence AS INTEGER) ASC LIMIT 2;" );
    $sth->execute( $trip_id );

    my $first_stop_id    = '';
    my $first_stop_name  = '';
    my $second_stop_id   = '';
    my $second_stop_name = '';

    @row = $sth->fetchrow_array();
    if ( scalar(@row) ) {
        $first_stop_id   = $row[0]   if ( $row[0]  );
        $first_stop_name = $row[1]   if ( $row[1]  );
    }
    @row = $sth->fetchrow_array();
    if ( scalar(@row) ) {
        $second_stop_id   = $row[0]   if ( $row[0]  );
        $second_stop_name = $row[1]   if ( $row[1]  );
    }

    if ( $first_stop_name && $second_stop_name &&
         $first_stop_name eq $second_stop_name    ) {

        $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_start='stop_name' WHERE trip_id=?;" );
        $sth->execute( $trip_id );
        $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_start) VALUES (?,'stop_name');" );
        $sth->execute( $trip_id );

        printf STDERR "Suspicious start per name for: %s\n", $trip_id  if ( $debug );

    } elsif ( $first_stop_id && $second_stop_id ) {

        if ( $first_stop_id eq $second_stop_id ) {

            $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_start='stop_id' WHERE trip_id=?;" );
            $sth->execute( $trip_id );
            $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_start) VALUES (?,'stop_id');" );
            $sth->execute( $trip_id );

            printf STDERR "Suspicious start per stop_id for: %s\n", $trip_id  if ( $debug  );

        } elsif ( $first_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {
            # check whether stop_ids are of type IFOPT ("a:b:c:d:e") and are equal on "a:b:c"
            my $string1 = $1 . ':' . $2 . ':' . $3;

            if ( $second_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {

                my $string2 = $1 . ':' . $2 . ':' . $3;

                if (  $string2 eq $string1 ) {

                    $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_start='stop_id' WHERE trip_id=?;" );
                    $sth->execute( $trip_id );
                    $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_start) VALUES (?,'stop_id');" );
                    $sth->execute( $trip_id );

                    printf STDERR "Suspicious start per IFOPT for: %s\n", $trip_id  if ( $debug );
                }
            }
        }
    }

    $dbh->commit();

}


#############################################################################################
#
# check for a suspicious end of this route. I.e. does the bus, ...
# make a u-turn at the end of the journey with/without passengers?
#
#

sub MarkSuspiciousEnd {
    my $trip_id = shift || '-';

    my $sth     = undef;
    my @row     = ();

    # we are only interested in the last two stops of this trip

    $sth = $dbh->prepare( "SELECT   stop_times.stop_id,stops.stop_name
                           FROM     stop_times
                           JOIN     stops ON stop_times.stop_id = stops.stop_id
                           WHERE    trip_id=?
                           ORDER BY CAST (stop_times.stop_sequence AS INTEGER) DESC LIMIT 2;" );
    $sth->execute( $trip_id );

    my $last_stop_id          = '';
    my $last_stop_name        = '';
    my $second_last_stop_id   = '';
    my $second_last_stop_name = '';

    @row = $sth->fetchrow_array();
    if ( scalar(@row) ) {
        $last_stop_id   = $row[0]   if ( $row[0]  );
        $last_stop_name = $row[1]   if ( $row[1]  );
    }
    @row = $sth->fetchrow_array();
    if ( scalar(@row) ) {
        $second_last_stop_id   = $row[0]   if ( $row[0]  );
        $second_last_stop_name = $row[1]   if ( $row[1]  );
    }

    if ( $last_stop_name && $second_last_stop_name &&
         $last_stop_name eq $second_last_stop_name    ) {

        $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_end='stop_name' WHERE trip_id=?;" );
        $sth->execute( $trip_id );
        $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_end) VALUES (?,'stop_name');" );
        $sth->execute( $trip_id );

        printf STDERR "Suspicious end per name for: %s\n", $trip_id  if ( $debug  );

    } elsif ( $last_stop_id && $second_last_stop_id ) {

        if ( $last_stop_id eq $second_last_stop_id ) {

            $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_end='stop_id' WHERE trip_id=?;" );
            $sth->execute( $trip_id );
            $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_end) VALUES (?,'stop_id');" );
            $sth->execute( $trip_id );

            printf STDERR "Suspicious end per stop_id for: %s\n", $trip_id  if ( $debug  );

        } elsif ( $second_last_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {
            # check whether stop_ids are of type IFOPT ("a:b:c:d:e") and are equal on "a:b:c"
            my $string1 = $1 . ':' . $2 . ':' . $3;

            if ( $last_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {

                my $string2 = $1 . ':' . $2 . ':' . $3;

                if (  $string2 eq $string1 ) {

                    $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_end='IFOPT' WHERE trip_id=?;" );
                    $sth->execute( $trip_id );
                    $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_end) VALUES (?,'IFOPT');" );
                    $sth->execute( $trip_id );

                    printf STDERR "Suspicious end per IFOPT for: %s\n", $trip_id  if ( $debug  );
                }
            }
        }
    }

    $dbh->commit();

}


#############################################################################################
#
# check for suspicious number of stops for vehicles other than ferries, funicular, aerialway
# Only 2 Stops? Is this really a route of a trip for the empty bus to the next job
#

sub MarkSuspiciousNumberOfStops {
    my $trip_id = shift || '-';

    my $sth     = undef;
    my @row     = ();

    # we are only interested in the number of stops for vehicles other than ferries, funicular, aerialway

    $sth = $dbh->prepare( "SELECT   route_type
                           FROM     routes
                           JOIN     trips ON routes.route_id = trips.route_id
                           WHERE    trip_id=?;" );
    $sth->execute( $trip_id );

    @row = $sth->fetchrow_array();

    if ( scalar(@row) && !exists($route_type_may_have_only_two_stops{$row[0]}) ) {

        $sth = $dbh->prepare( "SELECT   COUNT(stop_times.stop_id)
                               FROM     stop_times
                               JOIN     stops ON stop_times.stop_id = stops.stop_id
                               WHERE    trip_id=?;" );
        $sth->execute( $trip_id );

        @row = $sth->fetchrow_array();
        if ( scalar(@row) && $row[0] <= 2 ) {
            $sth = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_number_of_stops=? WHERE trip_id=?;" );
            $sth->execute( $row[0], $trip_id );
            $sth = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (suspicious_number_of_stops,trip_id) VALUES (?,?);" );
            $sth->execute( $row[0], $trip_id );

            printf STDERR "Suspicious number of stops for: %s\n", $trip_id  if ( $debug  );

            $dbh->commit();
        }
    }
}


#############################################################################################
#
# check for suspicious travel time of trip
#

sub MarkSuspiciousTripDuration {
    my $trip_id = shift || '-';

    my $sthd    = undef;
    my $stha    = undef;
    my @rowd     = ();
    my @rowa     = ();

    $sthd = $dbh->prepare( "SELECT   departure_time
                            FROM     stop_times
                            WHERE    trip_id=?
                            ORDER BY CAST (stop_sequence AS INTEGER) ASC
                            LIMIT 1;" );
    $sthd->execute( $trip_id );

    $stha = $dbh->prepare( "SELECT   arrival_time
                            FROM     stop_times
                            WHERE    trip_id=?
                            ORDER BY CAST (stop_sequence AS INTEGER) DESC
                            LIMIT 1;" );
    $stha->execute( $trip_id );

    @rowd = $sthd->fetchrow_array();
    @rowa = $stha->fetchrow_array();
    if ( scalar(@rowd) && $rowd[0] &&
         scalar(@rowa) && $rowa[0] &&
         $rowd[0] eq $rowa[0]         ) {
        $sthd = $dbh->prepare( "UPDATE OR IGNORE ptna_trips_comments SET suspicious_trip_duration='0:00' WHERE trip_id=?;" );
        $sthd->execute( $trip_id );
        $sthd = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (trip_id,suspicious_trip_duration) VALUES (?,'0:00');" );
        $sthd->execute( $trip_id );

        printf STDERR "Suspicious trip duration for: %s\n", $trip_id  if ( $debug  );

        $dbh->commit();
    }
}


#############################################################################################
#
#
#

sub FindNumberOfRidesForTripIds {

    my %service_id_service_days = ();
    my $hash_ref                = undef;
    my @row                     = ();
    my $service_id              = undef;
    my $start_date              = undef;
    my $end_date                = undef;
    my $service_days            = undef;
    my $on_days_of_week         = undef;
    my $trip_id                 = undef;
    my $list_service_ids        = undef;
    my @array_service_ids       = ();
    my $rides                   = undef;

    my $sthC  = $dbh->prepare( "SELECT   *
                                FROM     calendar;" );

    my $sthCA = $dbh->prepare( "SELECT   COUNT(exception_type)
                                FROM     calendar_dates
                                WHERE    service_id=? AND exception_type=1;" );

    my $sthCN = $dbh->prepare( "SELECT   COUNT(exception_type)
                                FROM     calendar_dates
                                WHERE    service_id=? AND exception_type=2;" );

    my $sthUR = $dbh->prepare( "UPDATE   ptna_trips SET rides=? WHERE trip_id=?;" );

    my $sthP  = $dbh->prepare( "SELECT   trip_id, list_service_ids
                                FROM     ptna_trips;" );

    #
    # for each service_id, calculate the number of days where the service is provided
    #
    $sthC->execute();

    #printf STDERR "%s FindNumberOfRidesForTripIds() - calculate service days\n", get_time();
    while ( $hash_ref = $sthC->fetchrow_hashref() ) {
        $service_id = $hash_ref->{'service_id'};
        $start_date = $hash_ref->{'start_date'};
        $end_date   = $hash_ref->{'end_date'};
        $service_days = Delta_Days(substr($start_date,0,4),substr($start_date,4,2),substr($start_date,6,2),
                                   substr($end_date,0,4),  substr($end_date,4,2),  substr($end_date,6,2)    );
        #printf STDERR "FindNumberOfRidesForTripIds: %s - %s = %d days\n", $start_date, $end_date, $service_days;
        $on_days_of_week  = $hash_ref->{'monday'};
        $on_days_of_week += $hash_ref->{'tuesday'};
        $on_days_of_week += $hash_ref->{'wednesday'};
        $on_days_of_week += $hash_ref->{'thursday'};
        $on_days_of_week += $hash_ref->{'friday'};
        $on_days_of_week += $hash_ref->{'saturday'};
        $on_days_of_week += $hash_ref->{'sunday'};
        $service_days     = ceil( $service_days * $on_days_of_week / 7 );
        if ( $service_days < 1 && $on_days_of_week > 0 ) {
            $service_days = $on_days_of_week;
        }
        #printf STDERR "FindNumberOfRidesForTripIds: %s - %s = %d days with %d of week\n", $start_date, $end_date, $service_days, $on_days_of_week;

        $sthCA->execute($service_id);
        @row  = $sthCA->fetchrow_array();
        if ( scalar(@row) ) {
            $service_days += $row[0];
        }
        $sthCN->execute($service_id);
        @row  = $sthCN->fetchrow_array();
        if ( scalar(@row) ) {
            $service_days -= $row[0];
        }
        $service_id_service_days{$service_id} = $service_days;
        #printf STDERR "FindNumberOfRidesForTripIds: \$service_id_service_days{%s} = %d\n", $service_id, $service_id_service_days{$service_id};
    }

    #
    # foreach representative trip_id take the service_ids of the represented trips and calculate the number of rides
    # service_ids will appear multiple times, each trip_id defines a single ride
    # store the number of rides per representatice trip_id in the db
    #
    $sthP->execute();

    #printf STDERR "%s FindNumberOfRidesForTripIds() - calculate rides\n", get_time();
    while ( $hash_ref = $sthP->fetchrow_hashref() ) {
        $trip_id          = $hash_ref->{'trip_id'};
        $list_service_ids = $hash_ref->{'list_service_ids'};
        #printf STDERR "FindNumberOfRidesForTripIds: \$list_service_ids{%s} = %s\n", $trip_id, $list_service_ids;
        $rides            = 0;
        if ( $list_separator eq '|' ) {
            @array_service_ids = split('\|',$list_service_ids)
        } else {
            @array_service_ids = split($list_separator,$list_service_ids)
        }
        foreach my $service_id ( @array_service_ids ) {
        #    printf STDERR "    %s: %d += %d\n", $service_id, $rides, $service_id_service_days{$service_id};
            if ( $service_id_service_days{$service_id} ) {      # key might not exist
                $rides += $service_id_service_days{$service_id};
            }
        }
        #printf STDERR "    \$rides{%s} = %d\n", $trip_id, $rides;
        $sthUR->execute($rides,$trip_id);
    }

    $dbh->commit();

}


#############################################################################################
#
#
# sum up the rides provided by the longest trip
# longest trip: is not sub-route of another trip
#               has sub-routes, add their "rides": "sum_rides" = "rides" + sum("rides" of sub-routes)
#

sub CalculateSumRidesOfLongestTrip {

    my $hash_refT   = undef;
    my $hash_refSR  = undef;
    my $route_id    = undef;
    my $trip_id     = undef;
    my $rowid       = 0;
    my $rides       = 0;
    my $sum_rides   = 0;
    my $updated     = 0;

    my $sthUS = $dbh->prepare( "UPDATE    ptna_trips SET sum_rides=? WHERE trip_id=?;" );

    my $sthT  = $dbh->prepare( "SELECT    rowid, trip_id, route_id, rides
                                FROM      ptna_trips
                                WHERE     trip_id NOT IN (SELECT trip_id FROM ptna_trips_comments WHERE subroute_of != '');" );

    my $sthSR = $dbh->prepare( "SELECT    SUM(rides) AS sum_rides
                                FROM      ptna_trips
                                LEFT JOIN ptna_trips_comments ON ptna_trips.trip_id = ptna_trips_comments.trip_id
                                WHERE     ptna_trips.route_id = ? AND subroute_of != '' AND (subroute_of=? OR subroute_of LIKE ? OR subroute_of LIKE ? OR subroute_of LIKE ?);" );

    #
    # select all representative trip_ids which are not a sub-route
    #
    $sthT->execute();

    while ( $hash_refT = $sthT->fetchrow_hashref() ) {
        $rowid       = $hash_refT->{'rowid'};
        $route_id    = $hash_refT->{'route_id'};
        $trip_id     = $hash_refT->{'trip_id'};
        $rides       = $hash_refT->{'rides'}       || 0;
        #printf STDERR "CalculateSumRidesOfLongestTrip: %s - %s -> rides = %d, subroute_of = '%s'\r", $route_id, $trip_id, $rides, $subroute_of;

        $sthSR->execute( $route_id, $trip_id, $trip_id.',%', '%,'.$trip_id.',%', '%,'.$trip_id);
        while ( $hash_refSR = $sthSR->fetchrow_hashref() ) {
            $sum_rides = $hash_refSR->{'sum_rides'} || 0;
            if ( $sum_rides > 0 ) {
                $sum_rides += $rides;
                $sthUS->execute( $sum_rides, $trip_id );
                $updated++;
                printf STDERR "%6d: %s -> rides = %d, sum_rides = %d%20s\r", $rowid, $trip_id, $rides, $sum_rides, ' ';
            }
        }
    }
    if ( $updated ) {
        printf STDERR "\n";
    }

    $dbh->commit();

}


#############################################################################################
#
# check similarities and differences between trips of same route
#

sub MarkTripsOfRouteId {
    my $route_id         = shift;
    my $details_hash_ref = shift;

    my @trip_ids = keys( %{$details_hash_ref} );
    my $last_len = 1;
    my $printstring = ' ';

    my @subroute_of = ();
    my @same_names_but_different_ids = ();
    my @same_stops_but_different_shape_ids = ();

    my $sthI = $dbh->prepare( "INSERT OR IGNORE INTO ptna_trips_comments (subroute_of,same_names_but_different_ids,same_stops_but_different_shape_ids,trip_id) VALUES (?,?,?,?);" );

    foreach my $trip_id (@trip_ids) {
        @subroute_of = ();
        @same_names_but_different_ids = ();
        @same_stops_but_different_shape_ids = ();
        foreach my $compare_with_trip_id (@trip_ids) {
            if ( $trip_id ne $compare_with_trip_id ) {
                #$printstring = sprintf "Route-Id %s: compare trip-id %s with %s", $route_id, $trip_id, $compare_with_trip_id;
                #printf STDERR "%${last_len}s%20s\r%s\r", ' ', ' ', $printstring;
                #$last_len = length( $printstring );

                if  ( $details_hash_ref->{$trip_id}{'stop_ids'} ne $details_hash_ref->{$compare_with_trip_id}{'stop_ids'} ) {
                    if ( $details_hash_ref->{$compare_with_trip_id}{'stop_ids'} =~ m/\Q$details_hash_ref->{$trip_id}{'stop_ids'}\E/ ) {
                        #printf STDERR "Route-Id %s: trip %s is sub-route of %s\n", $route_id, $trip_id, $compare_with_trip_id;
                        push( @subroute_of, $compare_with_trip_id );
                    }
                    if ( $details_hash_ref->{$trip_id}{'stop_names'} eq $details_hash_ref->{$compare_with_trip_id}{'stop_names'} ) {
                        #printf STDERR "Route-Id %s: Same stop names but different stop ids for %s and %s\n", $route_id, $trip_id, $compare_with_trip_id;
                        push( @same_names_but_different_ids, $compare_with_trip_id );
                    }
                } else {
                    if ( $details_hash_ref->{$trip_id}{'stop_names'} eq $details_hash_ref->{$compare_with_trip_id}{'stop_names'} ) {
                        #printf STDERR "Route-Id %s: Same stop names and stop ids but different shape ids for %s and %s\n", $route_id, $trip_id, $compare_with_trip_id;
                        push( @same_stops_but_different_shape_ids, $compare_with_trip_id );
                    }
                }
            }
        }
        if ( scalar(@subroute_of)                        ||
             scalar(@same_names_but_different_ids)       ||
             scalar(@same_stops_but_different_shape_ids)    ) {
            $sthI->execute( join(',',@subroute_of),
                            join(',',@same_names_but_different_ids),
                            join(',',@same_stops_but_different_shape_ids),
                            $trip_id );
        }
    }
    #printf STDERR "\n";
}


#############################################################################################
#
#
#

sub CreatePtnaAnalysis {

    my $sth = undef;
    my @row = ();

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );

    $sth = $dbh->prepare( "DROP TABLE IF EXISTS ptna_analysis;" );
    $sth->execute();

    $sth = $dbh->prepare( "CREATE TABLE ptna_analysis (
                                        'id'          INTEGER DEFAULT 0 PRIMARY KEY,
                                        'date'        TEXT,
                                        'duration'    INTEGER DEFAULT 0 );"
                         );
    $sth->execute();

    $sth = $dbh->prepare( "INSERT INTO ptna_analysis
                                  (id,date)
                           VALUES (1, ?);" );
    $sth->execute( $today );

    $sth   = $dbh->prepare( "UPDATE ptna SET analyzed=? WHERE id=1;" );
    $sth->execute( $today );

    $dbh->commit();

    return;
}


####################################################################################################################
#
#
#

sub ClearAllPtnaCommentsForTrips {

    my $sth = $dbh->prepare( "DELETE FROM ptna_trips_comments;" );

    $sth->execute();

    $dbh->commit();

}


#############################################################################################
#
#
#

sub UpdatePtnaAnalysis {
    my $seconds    = shift || 1;

    my $stmt    = '';
    my $sth     = undef;
    my @row     = ();

    $stmt = sprintf( "UPDATE ptna_analysis SET duration=%d WHERE id=1;", $seconds );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $dbh->commit();

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
