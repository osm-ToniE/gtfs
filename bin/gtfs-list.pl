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
my %STOPS               = ();   # key: 'stop_id'


#############################################################################################

use Getopt::Long;

my $debug                           = undef;
my $verbose                         = undef;
my $ifopt_levels                    = 5;
my $list_this                       = undef;
my $filename_routes                 = "routes.txt";
my $filename_stops                  = "stops.txt";

GetOptions( 'debug'                             =>  \$debug,                        # --debug
            'verbose'                           =>  \$verbose,                      # --verbose
            'ifopt-levels=i'                    =>  \$ifopt_levels,                 # --ifopt-levels=   --ifopt-levels=1 / --ifopt-levels=2 / --ifopt-levels=3 / --ifopt-levels=4 / --ifopt-levels=5
            'list=s'                            =>  \$list_this,                    # --list=           --list=ifopt / --list=IFOPT / --list=routes
            'routes=s'                          =>  \$filename_routes,              # --routes=
            'stops=s'                           =>  \$filename_stops,               # --stops=
          );


#############################################################################################

if ( $list_this ) {

    if ( $list_this =~ m/^ifopt$/i ) {

        read_stops( $filename_stops );

        write_ifopt();

    } elsif ( $list_this eq 'routes' ) {

        read_routes( $filename_routes );

        if ( scalar( keys( %ROUTE_SHORT_NAME ) ) ) {

            write_routes();

        } else {
            printf STDERR "No routes found in file %s\n", $filename_routes;
        }
    }
} else {

    printf STDERR "Please specify: --list=ifopt [--ifopt-levels=1|2|3|4|5] or --list=routes\n";

}


#############################################################################################
#
# convert data GTFS stops.txt file to an input file for JOSM (.osm)
#
# stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,platform_code,stop_url
# de:09162:813:1:1,Am Messesee,48.1358407908852,11.6903238314922,,,,http://efa.mvv-muenchen.de/mvv/XSLT_TRIP_REQUEST2?language=de&placeState_origin=empty&type_origin=stopID&name_origin=813&nameState_origin=empty&sessionID=0
# de:09173:4744:0:2,"Geretsried, Neuer Platz",47.8584032552367,11.4781560505183,,,,http://efa.mvv-muenchen.de/mvv/XSLT_TRIP_REQUEST2?language=de&placeState_origin=empty&type_origin=stopID&name_origin=4744&nameState_origin=empty&sessionID=0
#

sub read_stops {
    my $filename    = shift;

    my $stop_id = undef;
    my $name    = undef;
    my $lat     = undef;
    my $lon     = undef;

    if ( open(STOPS,$filename) ) {
        binmode STOPS, ":utf8";

        while ( <STOPS> ) {

            if ( m/^stop_id,/ ) {
                ;
            } elsif ( m/^(.*?),"(.*?)",([\-+0-9\.]+),([\-+0-9\.]+),/  ||
                      m/^(.*?),(.*?),([\-+0-9\.]+),([\-+0-9\.]+),/       ) {
                $stop_id = $1;
                $name    = $2;
                $lat     = $3;
                $lon     = $4;
                $name =~ s/&/\&amp;/g;
                $name =~ s/'/\&apos;/g;
                $STOPS{$stop_id}->{'lat'}               = $lat;
                $STOPS{$stop_id}->{'lon'}               = $lon;
                $STOPS{$stop_id}->{'name'}              = $name;
                $STOPS{$stop_id}->{'route_short_names'} = ();
            } else {
                printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
            }
        }
        close( STOPS );
    } else {
        printf STDERR "Failed to open 'stops' file: %s\n", $filename;
    }
}


#############################################################################################
#
#
#

sub write_ifopt {

    my %printed         = ();
    my @ifopt_parts     = ();
    my $ifopt_part      = undef;

    foreach my $ifopt ( sort ( keys( %STOPS ) ) ) {

        if ( $ifopt_levels < 1 || $ifopt_levels > 4 ) {

            printf STDOUT "%s\n", $ifopt;

        } else {

            @ifopt_parts = split( ':', $ifopt );
            $ifopt_part  = undef;

            if ( $ifopt_levels == 1 && defined($ifopt_parts[0]) ) {
                $ifopt_part = $ifopt_parts[0];
            } elsif ( $ifopt_levels == 2 && defined($ifopt_parts[0]) && defined($ifopt_parts[1]) ) {
                $ifopt_part = $ifopt_parts[0] . ':' . $ifopt_parts[1];
            } elsif ( $ifopt_levels == 3 && defined($ifopt_parts[0]) && defined($ifopt_parts[1]) && defined($ifopt_parts[2]) ) {
                $ifopt_part = $ifopt_parts[0] . ':' . $ifopt_parts[1] . ':' . $ifopt_parts[2];
            } elsif ( $ifopt_levels == 4 && defined($ifopt_parts[0]) && defined($ifopt_parts[1]) && defined($ifopt_parts[2]) && defined($ifopt_parts[3]) ) {
                $ifopt_part = $ifopt_parts[0] . ':' . $ifopt_parts[1] . ':' . $ifopt_parts[2] . ':' . $ifopt_parts[3];
            }

            if ( $ifopt_part ) {

                printf STDOUT "%s\n", $ifopt_part      unless ( $printed{$ifopt_part} );

                $printed{$ifopt_part} = 1;

            } else {
                
                printf STDERR "Can't split %s\n", $ifopt;
            }

        }
    }
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

