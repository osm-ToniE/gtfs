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


####################################################################################################################
#
#
#
####################################################################################################################

my $dbh = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { RaiseError => 1 } ) or die $DBI::errstr;


####################################################################################################################
#
#
#
####################################################################################################################

my ($sec,$min,$hour,$day,$month,$year) = localtime();

my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );
    

CreatePtnaAnalysis();


my $start_time              = time();

my @route_ids_of_agency     = FindRouteIdsOfAgency( $agency );

my @trip_ids_of_route_id    = ();

my %stop_hash_of_route_id   = ();

my $stop_list               = '';

printf STDERR "Routes of agancies selected: %d\n", scalar(@route_ids_of_agency)  if ( $verbose );

foreach my $route_id ( @route_ids_of_agency ) {
    
    @trip_ids_of_route_id  = FindTripIdsOfRouteId( $route_id );
    
    %stop_hash_of_route_id = ();

    foreach my $trip_id ( @trip_ids_of_route_id ) {
    
        $stop_list  = FindStopIdListAsString( $trip_id );

        $stop_hash_of_route_id{$stop_list} = $trip_id   if ( $stop_list );
    }
    
    printf STDERR "Route-ID: %s Trip-IDs: %s\n", $route_id, join( ', ', values(%stop_hash_of_route_id) )  if ( $verbose );
    
    AnalyzeStopIdLists( \%stop_hash_of_route_id );
}

UpdatePtnaAnalysis( time() - $start_time );

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
#
#

sub AnalyzeStopIdLists {
    my $hash_ref    = shift;
    
    my $stoplist1   = '';
    my $trip_id1    = '';
    my $stoplist2   = '';
    my $trip_id2    = '';
    
    my @subroute_of = ();
    
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
                my $stmt  = sprintf( "UPDATE trips SET ptna_changedate='%s',ptna_comment='Teilroute von %s' WHERE trip_id='%s'", $today, join(', ',@subroute_of), ${$hash_ref}{$stoplist1} );
                my $sth   = $dbh->prepare( $stmt );
                $sth->execute();
                
                printf STDERR "%s ist Teilroute of: %s\n", ${$hash_ref}{$stoplist1}, join( ', ', @subroute_of )  if ( $verbose );
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
                                   'id'                 INTEGER DEFAULT 0 PRIMARY KEY,
                                   'date'               TEXT,
                                   'duration'           INTEGER DEFAULT 0,
                                   'count_subroute'     INTEGER DEFAULT 0
                      );"
                   );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt               = sprintf( "INSERT INTO ptna_analysis 
                                           (id,date)
                                    VALUES (1, '%s');", 
                                                $today
                                 );
    $sth                = $dbh->prepare( $stmt );
    $sth->execute();
    
    $stmt  = sprintf( "UPDATE ptna SET analyzed='%s' WHERE id=1;", $today );
    $sth   = $dbh->prepare( $stmt );
    $sth->execute();

    return;
}


#############################################################################################
#
#
#

sub UpdatePtnaAnalysis {
    my $seconds    = shift || 0;
    
    my $stmt    = '';
    my $sth     = undef;
    my @row     = ();

    
    $stmt = sprintf( "SELECT COUNT(*) FROM trips  WHERE ptna_comment LIKE 'Teilroute von%%';" ); 
    $sth = $dbh->prepare( $stmt );
    $sth->execute();
    @row = $sth->fetchrow_array();

    $stmt = sprintf( "UPDATE ptna_analysis SET count_subroute='%s' WHERE id=1;", $row[0] );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "UPDATE ptna_analysis SET duration=%d WHERE id=1;", $seconds );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


