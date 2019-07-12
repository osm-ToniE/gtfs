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


#############################################################################################

my %ROUTE_SHORT_NAME    = ();


#############################################################################################

use Getopt::Long;

my $verbose                         = undef;
my $debug                           = undef;
my $filename_routes                 = "routes.txt";

GetOptions( 'debug'                             =>  \$debug,                        # --debug
            'verbose'                           =>  \$verbose,                      # --verbose
            'routes=s'                          =>  \$filename_routes,              # --routes=
          );


#############################################################################################

read_routes( $filename_routes );

if ( scalar( keys( %ROUTE_SHORT_NAME ) ) ) {

    write_routes( );
    
} else {
    printf STDERR "No routes found in file %s\n", $filename_routes;
}


#############################################################################################
#
# route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color
# 19-210-s19-1,1,210,"Neuperlach Süd - Ottobrunn - Taufkirchen, Lilienthalstraße - Brunnthal, Zusestraße",701,,008351,FFFFFF
# 19-211-s19-1,1,211,Campeon - Unterbiberg - Neubiberg - Putzbrunn - Harthausen,701,,008351,FFFFFF
#

sub read_routes {
    my $filename    = shift;
    
    my $route_id            = undef;
    my $route_short_name    = undef;

    if ( open(ROUTES,$filename) ) {
        binmode ROUTES, ":utf8";

        while ( <ROUTES> ) {
            
            if ( m/^route_id,/ ) {
                ;
            } elsif ( m/^(.*?),(.*?),(.*?),/ ) {
                $route_id = $1;
                $route_short_name  = $3;
                
                # the next one ensures that older versions of a route-id are not overwritten with a newer one
                
                # 19-447-s19-1,1,447,"Grafing Bahnhof  - Oberelkofen - Aßling - Aßling, Rathaus",701,,008351,FFFFFF
                # 19-447-s19-2,1,447,"Grafing Bahnhof  - Oberelkofen - Aßling - Aßling, Rathaus",701,,008351,FFFFFF
                # 19-447-s19-3,1,447,"Grafing Bahnhof  - Oberelkofen - Aßling - Aßling, Rathaus",701,,008351,FFFFFF

                $ROUTE_SHORT_NAME{$route_short_name}->{'route_id'} = $route_id;
            } else {
                printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
            }
        }
        close( ROUTES );
    } else {
        printf STDERR "Failed to open 'routes' file: %s\n", $filename;
    }
}


#############################################################################################
#
#
#

sub write_routes {

    foreach my $route_short_name ( sort ( keys( %ROUTE_SHORT_NAME ) ) ) {
        printf STDOUT "%s\n", $route_short_name;
    }

}

