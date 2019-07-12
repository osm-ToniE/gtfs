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

my %ROUTES              = ();   # key: 'route_id'
my %STOPS               = ();   # key: 'stop_id'
my %TRIPS               = ();   # key: 'trip_id'

my %ROUTE_SHORT_NAME    = ();


#############################################################################################

use Getopt::Long;

my $verbose                         = undef;
my $debug                           = undef;
my $filename_routes                 = "routes.txt";
my $filename_stops                  = "stops.txt";
my $filename_stop_times             = "stop_times.txt";
my $filename_trips                  = "trips.txt";

GetOptions( 'debug'                             =>  \$debug,                        # --debug
            'verbose'                           =>  \$verbose,                      # --verbose
            'routes=s'                          =>  \$filename_routes,              # --routes=
            'stops=s'                           =>  \$filename_stops,               # --stops=
            'stop-times=s'                      =>  \$filename_stop_times,          # --stop-times=
            'trips=s'                           =>  \$filename_trips,               # --trips=
          );


#############################################################################################

read_routes( $filename_routes );

read_stops( $filename_stops );

read_stop_times( $filename_stop_times );

read_trips( $filename_trips );

enhance_stops();

write_platforms();


#############################################################################################
#
# convert data GTFS routes.txt file to an input file for JOSM (.osm)
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
                $ROUTES{$route_id}->{'route_short_name'}           = $route_short_name;
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
# convert data GTFS stop_times.txt file to an input file for JOSM (.osm)
#
# trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,timepoint
# 72.T0.19-852-s19-1.2.R,06:46:00,06:46:00,de:09179:6250:1:2,1,0,0,
#

sub read_stop_times {
    my $filename    = shift;
    
    my $trip_id         = undef;
    my $stop_id         = undef;
    my $stop_sequence   = undef;

    if ( open(STOPTIMES,$filename) ) {
        binmode STOPTIMES, ":utf8";

        while ( <STOPTIMES> ) {
            
            if ( m/^trip_id,/ ) {
                ;
            } elsif ( m/^(.*?),(.*?),(.*?),(.*?),(.*?),/ ) {
                $trip_id = $1;
                $stop_id = $4;
                $stop_sequence = $5;
                if ( $TRIPS{$trip_id}->{'stop_id'}->{$stop_id} ) {
                    $TRIPS{$trip_id}->{'stop_id'}->{$stop_id} .= ',' . $stop_sequence;
                } else {
                    $TRIPS{$trip_id}->{'stop_id'}->{$stop_id}  = $stop_sequence;
                }
                if ( $STOPS{$stop_id}->{'trip_id'}->{$trip_id} ) {
                    $STOPS{$stop_id}->{'trip_id'}->{$trip_id} .= ',' . $stop_sequence;
                } else {
                    $STOPS{$stop_id}->{'trip_id'}->{$trip_id}  = $stop_sequence;
                }
            } else {
                printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
            }
        }
        close( STOPTIMES );
    } else {
        printf STDERR "Failed to open 'stop_times' file: %s\n", $filename;
    }
}


#############################################################################################
#
# convert data GTFS trips.txt file to an input file for JOSM (.osm)
#
# route_id,service_id,trip_id,trip_headsign
# 19-852-s19-1,19T0,68.T0.19-852-s19-1.7.R,Fürstenfeldbruck
#

sub read_trips {
    my $filename    = shift;
    
    my $route_id        = undef;
    my $trip_id         = undef;

    if ( open(TRIPS,$filename) ) {
        binmode TRIPS, ":utf8";

        while ( <TRIPS> ) {
            
            if ( m/^route_id,/ ) {
                ;
            } elsif ( m/^(.*?),"(.*?)",(.*?),/  || 
                      m/^(.*?),(.*?),(.*?),/       ) {
                $route_id = $1;
                $trip_id  = $3;
                $ROUTES{$route_id}->{'trip_id'}->{$trip_id} = 1;
                $TRIPS{$trip_id}->{'route_id'}->{$route_id} = 1;
            } else {
                printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
            }
        }
        close( TRIPS );
    } else {
        printf STDERR "Failed to open 'trips' file: %s\n", $filename;
    }
}


#############################################################################################
#
#
#

sub enhance_stops {
    foreach my $stop_id ( sort ( keys ( %STOPS ) ) ) {
        if ( $STOPS{$stop_id}->{'name'} && $STOPS{$stop_id}->{'lat'} && $STOPS{$stop_id}->{'lon'} ) {
            foreach my $trip_id ( keys( %{$STOPS{$stop_id}->{'trip_id'}} ) ) {
                map { $STOPS{$stop_id}->{'route_id'}->{$_} = 1; } keys ( %{$TRIPS{$trip_id}->{'route_id'}} );
            }
            foreach my $route_id ( keys( %{$STOPS{$stop_id}->{'route_id'}} ) ) {
                $STOPS{$stop_id}->{'route_short_names'}->{$ROUTES{$route_id}->{'route_short_name'}} = 1; 
            }
        }
    }
}


#############################################################################################
#
#
#

sub write_platforms {

    printf "<?xml version='1.0' encoding='UTF-8' ?>\r\n";
    printf "<gpx xmlns='http://www.topografix.com/GPX/1/1' version='1.0' creator='gtfs-to-platform-gpx.pl' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd'>\r\n";
    foreach my $stop_id ( sort ( keys ( %STOPS ) ) ) {
        if ( $STOPS{$stop_id}->{'name'} && $STOPS{$stop_id}->{'lat'} && $STOPS{$stop_id}->{'lon'} ) {
            printf "<wpt lat='%s' lon='%s'>\r\n", $STOPS{$stop_id}->{'lat'}, $STOPS{$stop_id}->{'lon'};
            printf "    <name><![CDATA[%s (Busses ~ '%s') @ IFOPT = %s]]></name>\r\n", $STOPS{$stop_id}->{'name'}, join(';',sort(keys(%{$STOPS{$stop_id}->{'route_short_names'}}))), $stop_id;
            printf "</wpt>\r\n";
        } else {
            printf STDERR "Missing information for STOP_ID: %s\n", $stop_id;
            foreach my $key ( sort ( keys ( %{$STOPS{$stop_id}} ) ) ) {
                printf STDERR "    \$STOPS{%s}->{%s} = %s\n", $stop_id, $key, $STOPS{$stop_id}->{$key};
            }
            printf STDERR "\n";
        }
    }
    printf "</gpx>\r\n";

}

