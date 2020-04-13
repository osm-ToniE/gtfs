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

my $debug                    = undef;
my $verbose                  = undef;
my $language                 = 'en';

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
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

my ($sec,$min,$hour,$day,$month,$year) = localtime();

my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );

my $start_time              = time();

CreatePtnaNormalization();

my $normalized_routes = NormalizeRouteLongName();

my $normalized_stops  = NormalizeStopName();

UpdatePtnaNormalization( time() - $start_time, $normalized_routes, $normalized_stops );

exit 0;


#############################################################################################
#
#
#

sub CreatePtnaNormalization {
    my $stmt         = '';
    my $sth          = undef;
    my @row          = ();

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );


    $stmt = sprintf( "DROP TABLE IF EXISTS ptna_normalization;" );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "CREATE TABLE ptna_normalization (
                                   'id'                    INTEGER DEFAULT 0 PRIMARY KEY,
                                   'date'                  TEXT,
                                   'duration'              INTEGER DEFAULT 0,
                                   'routes'                INTEGER DEFAULT 0,
                                   'stops'                 INTEGER DEFAULT 0
                      );"
                   );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt = sprintf( "INSERT INTO ptna_normalization
                             (id,date)
                      VALUES (1, '%s');",
                      $today
                   );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    $stmt  = sprintf( "UPDATE ptna SET normalized='%s' WHERE id=1;", $today );
    $sth   = $dbh->prepare( $stmt );
    $sth->execute();

    return;
}


#############################################################################################
#
#
#

sub UpdatePtnaNormalization {
    my $seconds    = shift || 1;
    my $routes     = shift || 0;
    my $stops      = shift || 0;

    my $stmt                    = '';
    my $sth                     = undef;
    $stmt = sprintf( "UPDATE ptna_normalization SET date='%s',duration='%s',routes='%s',stops='%s' WHERE id=1;", $today, $seconds, $routes, $stops );
    $sth  = $dbh->prepare( $stmt );
    $sth->execute();

    return 0;
}


#############################################################################################
#
#
#

sub NormalizeRouteLongName {
    my $stmt                    = '';
    my $sth                     = undef;
    my @row                     = ();
    my $original                = '';
    my $normalized              = '';
    my $route_id                = '';
    my $number_of_routes        = 0;
    my $number_of_normalized    = 0;

    printf STDERR "Routes normalized: %06d of %06d\r", $number_of_normalized, $number_of_routes     if ( $verbose );
    
    $stmt = sprintf( "SELECT COUNT(*) FROM routes;" );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

    if ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            $number_of_routes = $row[0];
        }
    }

    printf STDERR "Routes normalized: %06d of %06d\r", $number_of_normalized, $number_of_routes     if ( $verbose );
    
    $stmt = sprintf( "SELECT route_long_name,route_id FROM routes;" );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

    
    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0] && $row[1]  ) {
            $original = $row[0];
            $route_id = $row[1];
            
            $normalized = NormalizeString( $original );
            
            if ( $normalized ne $original ) {
                #$stmt = sprintf( "UPDATE routes SET normalized_route_long_name='%s' WHERE route_id='%s';", $normalized, $route_id );
                #$sth  = $dbh->prepare( $stmt );
                #$sth->execute();
                
                $number_of_normalized++;
                
                printf STDERR "Routes normalized: %06d of %06d\r", $number_of_normalized, $number_of_routes     if ( $verbose );
            }
        }
    }
    
    printf STDERR "Routes normalized: %06d of %06d\n", $number_of_normalized, $number_of_routes     if ( $verbose );
    
    return $number_of_normalized;
}


#############################################################################################
#
#
#

sub NormalizeStopName {
    my $stmt                    = '';
    my $sth                     = undef;
    my @row                     = ();
    my $original                = '';
    my $normalized              = '';
    my $stop_id                 = '';
    my $number_of_stops         = 0;
    my $number_of_normalized    = 0;

    printf STDERR "Stops  normalized: %06d of %06d\r", $number_of_normalized, $number_of_stops     if ( $verbose );
    
    $stmt = sprintf( "SELECT COUNT(*) FROM stops;" );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

    if ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            $number_of_stops = $row[0];
        }
    }

    printf STDERR "Stops  normalized: %06d of %06d\r", $number_of_normalized, $number_of_stops     if ( $verbose );
    
    $stmt = sprintf( "SELECT stop_name,stop_id FROM stops;" );

    $sth = $dbh->prepare( $stmt );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0] && $row[1]  ) {
            $original = $row[0];
            $stop_id  = $row[1];
            
            $normalized = NormalizeString( $original );
            
            if ( $normalized ne $original ) {
                #$stmt = sprintf( "UPDATE stops SET normalized_stop_name='%s' WHERE stop_id='%s';", $normalized, $stop_id );
                #$sth  = $dbh->prepare( $stmt );
                #$sth->execute();
                
                $number_of_normalized++;
                
                printf STDERR "Stops  normalized: %06d of %06d\r", $number_of_normalized, $number_of_stops     if ( $verbose );
            }
        }
    }
    
    printf STDERR "Stops  normalized: %06d of %06d\n", $number_of_normalized, $number_of_stops     if ( $verbose );
    
    return $number_of_normalized;
}


#############################################################################################
#
#
#

sub NormalizeString {
    my $original   = shift || '';
    my $normalized = $original;
    
    if ( $original ) {
        $normalized =~ s/Str\./Straße/g;
        $normalized =~ s/str\./straße/g;
        $normalized =~ s/Pl\./Platz/g;
        $normalized =~ s/Abzw\./Abzweig/g;
    }
    
    return $normalized;

}
    

