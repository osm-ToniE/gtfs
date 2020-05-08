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

my $subroute_of        = "Sub-route of";
my $suspicious_end_for = "Suspicious end of itinerary: does the vehicle make a u-turn with or without passengers";

if ( $language eq 'de' ) {
    $subroute_of        = "Teilroute von";
    $suspicious_end_for = "Verdächtiges Ende der Fahrt: wendet das Fahrzeug an der Endhaltestelle mit oder ohne Passagiere";
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


CreatePtnaAnalysis();

ClearAllPtnaCommentsForTrips();

my $start_time              = time();

my @route_ids_of_agency     = FindRouteIdsOfAgency( $agency );

my @trip_ids_of_route_id    = ();

my %stop_hash_of_route_id   = ();

my $stop_list               = '';

printf STDERR "Routes of agencies selected: %d\n", scalar(@route_ids_of_agency)  if ( $verbose > 1 );

foreach my $route_id ( @route_ids_of_agency ) {

    @trip_ids_of_route_id  = FindTripIdsOfRouteId( $route_id );

    %stop_hash_of_route_id = ();

    foreach my $trip_id ( @trip_ids_of_route_id ) {

        MarkSuspiciousEnd( $trip_id );

        $stop_list  = FindStopIdListAsString( $trip_id );

        $stop_hash_of_route_id{$stop_list} = $trip_id   if ( $stop_list );
    }

    printf STDERR "Route-ID: %s Trip-IDs: %s\n", $route_id, join( ', ', values(%stop_hash_of_route_id) )  if ( $verbose > 1 );

    MarkSubRoutes( \%stop_hash_of_route_id );
}

UpdatePtnaAnalysis( time() - $start_time );

exit 0;


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

sub FindStopIdListAsString {
    my $trip_id  = shift || '-';

    my $sth      = undef;
    my @row      = ();

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
# check for a suspicious end of this route. I.e. does the bus, ...
# make a u-turn at the end of the journey with/without passengers?
#
#

sub MarkSuspiciousEnd {
    my $trip_id  = shift || '-';

    my $sth      = undef;
    my @row      = ();

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

        $sth = $dbh->prepare( "UPDATE trips SET ptna_changedate=?,ptna_comment=? WHERE trip_id=?;" );
        $sth->execute( $today, $suspicious_end_for . ' (stop_name)?', $trip_id );

        printf STDERR "Suspicious end per name for: %s\n", $trip_id  if ( $verbose > 1 );

    } elsif ( $last_stop_id && $second_last_stop_id ) {

        # check whether stop_ids are of type IFOPT ("a:b:c:d:e") and are equal on "a:b:c"

        if ( $second_last_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {

            my $string1 = $1 . ':' . $2 . ':' . $3;

            if ( $last_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {

                my $string2 = $1 . ':' . $2 . ':' . $3;

                if (  $string2 eq $string1 ) {

                    $sth = $dbh->prepare( "UPDATE trips SET ptna_changedate=?,ptna_comment=? WHERE trip_id=?;" );
                    $sth->execute( $today, $suspicious_end_for . ' (IFOPT)?', $trip_id );

                    printf STDERR "Suspicious end per IFOPT for: %s\n", $trip_id  if ( $verbose > 1 );
                }
            }
        }
    }

    $dbh->commit();

}


#############################################################################################
#
# check whether we find sub-routes of this route here.
# I.e. a stop-list which is a sub-string of another stop-list
#

sub MarkSubRoutes {
    my $hash_ref        = shift;

    my $stoplist1       = '';
    my $stoplist2       = '';
    my @subroute_of     = ();

    if ( $hash_ref ) {

        my @stop_lists = keys( %{$hash_ref} );

        my $sthS = $dbh->prepare( "SELECT   ptna_comment
                                   FROM     trips
                                   WHERE    trip_id=?;" );

        my $sthU = $dbh->prepare( "UPDATE trips SET ptna_changedate=?,ptna_comment=? WHERE trip_id=?;" );

        foreach $stoplist1 (@stop_lists) {

            @subroute_of = ();

            foreach $stoplist2 (@stop_lists) {

                next if ( $stoplist1 eq $stoplist2 );

                if ( $stoplist2 =~ m/\Q$stoplist1\E/ ) {
                    push( @subroute_of, ${$hash_ref}{$stoplist2} );
                }
            }

            if ( scalar(@subroute_of) ) {

                $sthS->execute( ${$hash_ref}{$stoplist1} );

                my @row = $sthS->fetchrow_array();
                my $existing_comment = '';
                if ( $row[0] ) {
                        $existing_comment = $row[0] . "\n";
                }

                $sthU->execute( $today, $existing_comment . $subroute_of . ' ' . join(', ',@subroute_of), ${$hash_ref}{$stoplist1} );

                printf STDERR "%s is sub-route of: %s\n", ${$hash_ref}{$stoplist1}, join( ', ', @subroute_of )  if ( $verbose > 1 );
            }
        }
    }

    $dbh->commit();

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
                                        'id'                    INTEGER DEFAULT 0 PRIMARY KEY,
                                        'date'                  TEXT,
                                        'duration'              INTEGER DEFAULT 0,
                                        'count_subroute'        INTEGER DEFAULT 0,
                                        'count_suspicious_end'  INTEGER DEFAULT 0 );"
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

    my $sth = $dbh->prepare( "UPDATE trips SET ptna_changedate='',ptna_is_invalid='',ptna_is_wrong='',ptna_comment='';" );

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


    $stmt = sprintf( "SELECT COUNT(*) FROM trips  WHERE ptna_comment LIKE '%%%s%%';", $subroute_of );
    $sth = $dbh->prepare( $stmt );
    $sth->execute();
    @row = $sth->fetchrow_array();

    $stmt = sprintf( "UPDATE ptna_analysis SET count_subroute='%s' WHERE id=1;", $row[0] );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "SELECT COUNT(*) FROM trips  WHERE ptna_comment LIKE '%%%s%%';", $suspicious_end_for );
    $sth = $dbh->prepare( $stmt );
    $sth->execute();
    @row = $sth->fetchrow_array();

    $stmt = sprintf( "UPDATE ptna_analysis SET count_suspicious_end='%s' WHERE id=1;", $row[0] );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "UPDATE ptna_analysis SET duration=%d WHERE id=1;", $seconds );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $dbh->commit();

    return 0;
}
