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
my $language                 = 'de';
my $agency                   = undef;

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'language=s'            =>  \$language,              # --language=de
            'agency=s'              =>  \$agency,                # --agency=
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

my $dbh = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { RaiseError => 1 } ) or die $DBI::errstr;


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

sub FindTripIdsOfRouteId {
    my $route_id     = shift || '-';

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();
    my @return_array = ();

    $stmt = sprintf( "SELECT DISTINCT trips.trip_id
                      FROM            trips
                      WHERE           trips.route_id='%s';",
                      $route_id );

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
# check for a suspicious end of this route. I.e. does the bus, ...
# make a u-turn at the end of the journey with/without passengers?
#
#

sub MarkSuspiciousEnd {
    my $trip_id      = shift || '-';

    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    # we are only interested in the last two stops of this trip

    $stmt = sprintf( "SELECT   stop_times.stop_id,stops.stop_name
                      FROM     stop_times
                      JOIN     stops ON stop_times.stop_id = stops.stop_id
                      WHERE    trip_id='%s'
                      ORDER BY CAST (stop_times.stop_sequence AS INTEGER) DESC LIMIT 2;",
                      $trip_id
                   );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

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

        $stmt  = sprintf( "UPDATE trips SET ptna_changedate='%s',ptna_comment='%s (stop_name)?' WHERE trip_id='%s'", $today, $suspicious_end_for, $trip_id );
        $sth   = $dbh->prepare( $stmt );
        $sth->execute();

        printf STDERR "Suspicious end per name for: %s\n", $trip_id  if ( $verbose );

    } elsif ( $last_stop_id && $second_last_stop_id ) {

        # check whether stop_ids are of type IFOPT ("a:b:c:d:e") and are equal on "a:b:c"

        if ( $second_last_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {

            my $string1 = $1 . ':' . $2 . ':' . $3;

            if ( $last_stop_id =~ m/^(.+):(.+):(.+):(.+):(.+)$/ ) {

                my $string2 = $1 . ':' . $2 . ':' . $3;

                if (  $string2 eq $string1 ) {

                    $stmt  = sprintf( "UPDATE trips SET ptna_changedate='%s',ptna_comment='%s (IFOPT)?' WHERE trip_id='%s'", $today, $suspicious_end_for, $trip_id );
                    $sth   = $dbh->prepare( $stmt );
                    $sth->execute();

                    printf STDERR "Suspicious end per IFOPT for: %s\n", $trip_id  if ( $verbose );
                }
            }
        }
    }
}


#############################################################################################
#
# check whether we find sub-routes of this route here.
# I.e. a stop-list which is a sub-string of another stop-list
#

sub MarkSubRoutes {
    my $hash_ref    = shift;

    my $stoplist1           = '';
    my $stoplist2           = '';
    my @subroute_of         = ();

    if ( $hash_ref ) {

        my @stop_lists = keys( %{$hash_ref} );

        foreach $stoplist1 (@stop_lists) {

            @subroute_of = ();

            foreach $stoplist2 (@stop_lists) {

                next if ( $stoplist1 eq $stoplist2 );

                if ( $stoplist2 =~ m/\Q$stoplist1\E/ ) {
                    push( @subroute_of, ${$hash_ref}{$stoplist2} );
                }
            }

            if ( scalar(@subroute_of) ) {

                my $stmt = sprintf( "SELECT   ptna_comment
                                     FROM     trips
                                     WHERE    trip_id='%s';",
                                     ${$hash_ref}{$stoplist1}
                                  );

                my $sth = $dbh->prepare( $stmt );
                $sth->execute();

                my @row = $sth->fetchrow_array();
                my $existing_comment = '';
                if ( $row[0] ) {
                        $existing_comment = $row[0] . "\n";
                }

                $stmt  = sprintf( "UPDATE trips SET ptna_changedate='%s',ptna_comment='%s%s %s' WHERE trip_id='%s'", $today, $existing_comment, $subroute_of, join(', ',@subroute_of), ${$hash_ref}{$stoplist1} );
                $sth   = $dbh->prepare( $stmt );
                $sth->execute();

                printf STDERR "%s is sub-route of: %s\n", ${$hash_ref}{$stoplist1}, join( ', ', @subroute_of )  if ( $verbose > 1 );
            }
        }
    }
}


#############################################################################################
#
#
#

sub CreatePtnaAnalysis {
    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );


    $stmt = sprintf( "DROP TABLE IF EXISTS ptna_analysis;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "CREATE TABLE ptna_analysis (
                                   'id'                    INTEGER DEFAULT 0 PRIMARY KEY,
                                   'date'                  TEXT,
                                   'duration'              INTEGER DEFAULT 0,
                                   'count_subroute'        INTEGER DEFAULT 0,
                                   'count_suspicious_end'  INTEGER DEFAULT 0
                      );"
                   );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "INSERT INTO ptna_analysis
                             (id,date)
                      VALUES (1, '%s');",
                      $today
                   );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt  = sprintf( "UPDATE ptna SET analyzed='%s' WHERE id=1;", $today );
    $sth   = $dbh->prepare( $stmt );
    $sth->execute();

    return;
}


####################################################################################################################
#
#
#

sub ClearAllPtnaCommentsForTrips {

    my $stmt    = '';
    my $sth     = undef;
    my @row     = ();


    $stmt = sprintf( "UPDATE trips SET ptna_changedate='',ptna_is_invalid='',ptna_is_wrong='',ptna_comment='';" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

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


    $stmt = sprintf( "SELECT COUNT(*) FROM trips  WHERE ptna_comment LIKE '%s%%';", $subroute_of );
    $sth = $dbh->prepare( $stmt );
    $sth->execute();
    @row = $sth->fetchrow_array();

    $stmt = sprintf( "UPDATE ptna_analysis SET count_subroute='%s' WHERE id=1;", $row[0] );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "SELECT COUNT(*) FROM trips  WHERE ptna_comment LIKE '%s%%';", $suspicious_end_for );
    $sth = $dbh->prepare( $stmt );
    $sth->execute();
    @row = $sth->fetchrow_array();

    $stmt = sprintf( "UPDATE ptna_analysis SET count_suspicious_end='%s' WHERE id=1;", $row[0] );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "UPDATE ptna_analysis SET duration=%d WHERE id=1;", $seconds );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


