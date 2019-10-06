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
my $stop_id_is                      = '';

GetOptions( 'debug'                             =>  \$debug,                        # --debug
            'verbose'                           =>  \$verbose,                      # --verbose
            'stop-id-is=s'                      =>  \$stop_id_is,                   # --stop-id-is="ref:IFOPT" or "uic_ref"
            'routes=s'                          =>  \$filename_routes,              # --routes=
            'stops=s'                           =>  \$filename_stops,               # --stops=
            'stop-times=s'                      =>  \$filename_stop_times,          # --stop-times=
            'trips=s'                           =>  \$filename_trips,               # --trips=
          );


#############################################################################################

if ( $stop_id_is ) {
    if ( $stop_id_is =~ m|^[0-9A-Za-z\:\._/\+\-]+$| ) {
        $stop_id_is = $stop_id_is . ' = ';
    } else {
        printf STDERR "%s: wrong value for option: --stop-id-is=%s\n", $0, $stop_id_is;
    }
}

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
    my $filename            = shift;

    my $route_id            = undef;
    my $agency_id           = undef;
    my $route_short_name    = undef;
    my @cells               = ();

    if ( open(ROUTES,$filename) ) {
        binmode ROUTES, ":utf8";

        while ( <ROUTES> ) {

            if ( m/^route_id,/ ) {
                ;
            } else {
                @cells              = parse_csv( ',', $_ );
                $route_id           = $cells[0];
                $agency_id          = $cells[1];
                $route_short_name   = $cells[2];
                if ( defined $route_id && defined $agency_id && defined $route_short_name ) {
                    # printf STDERR "read_routes: '%s' '%s' '%s'\n", $route_id, $agency_id, $route_short_name;
                    $ROUTES{$route_id}->{'route_short_name'}           = $route_short_name;
                    $ROUTE_SHORT_NAME{$route_short_name}->{'route_id'} = $route_id;
                } else {
                    printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
                }
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
# from DE-BY-MVV - w/o stop_code and w/o stop_desc
# stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,platform_code,stop_url
# de:09162:813:1:1,Am Messesee,48.1358407908852,11.6903238314922,,,,http://efa.mvv-muenchen.de/mvv/XSLT_TRIP_REQUEST2?language=de&placeState_origin=empty&type_origin=stopID&name_origin=813&nameState_origin=empty&sessionID=0
# de:09173:4744:0:2,"Geretsried, Neuer Platz",47.8584032552367,11.4781560505183,,,,http://efa.mvv-muenchen.de/mvv/XSLT_TRIP_REQUEST2?language=de&placeState_origin=empty&type_origin=stopID&name_origin=4744&nameState_origin=empty&sessionID=0
#
# from LU - w/ stop_code and w/ stop_desc
# stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,stop_timezone,wheelchair_boarding,platform_code
# 150604002,,"Niederpallen, Veräinsbau",,49.754886,5.911422,,,0,,,0,
# 150604001,,"Niederpallen, Ditzebierg",,49.755589,5.908100,,,0,,,0,
#

sub read_stops {
    my $filename    = shift;

    my $stop_id     = undef;
    my $name        = undef;
    my $lat         = undef;
    my $lon         = undef;
    my $name_pos    = 0;
    my $lat_pos     = 0;
    my $lon_pos     = 0;
    my @cells       = ();

    if ( open(STOPS,$filename) ) {
        binmode STOPS, ":utf8";

        while ( <STOPS> ) {

            if ( m/^stop_id,/ ) {
                @cells = split( ',' );

                for ( my $i = 0; $i < scalar(@cells); $i++ ) {
                    $name_pos = $i  if ( $cells[$i] =~ m/stop_name/ );
                    $lat_pos  = $i  if ( $cells[$i] =~ m/stop_lat/ );
                    $lon_pos  = $i  if ( $cells[$i] =~ m/stop_lon/ );
                }

            } else {
                @cells = parse_csv( ',', $_ );
                $stop_id = $cells[0];
                $name    = $cells[$name_pos];
                $lat     = $cells[$lat_pos];
                $lon     = $cells[$lon_pos];
                if ( defined $stop_id && defined $name && defined $lat && defined $lon ) {
                    $name =~ s/&/\&amp;/g;
                    $name =~ s/'/\&apos;/g;
                    #printf STDERR "read_stops: '%s' '%s' '%s' '%s'\n", $stop_id, $name, $lat, $lon;
                    $STOPS{$stop_id}->{'lat'}               = $lat;
                    $STOPS{$stop_id}->{'lon'}               = $lon;
                    $STOPS{$stop_id}->{'name'}              = $name;
                    $STOPS{$stop_id}->{'route_short_names'} = ();
                } else {
                    printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
                }
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
# from DE-BY-MMM
# trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,timepoint
# 72.T0.19-852-s19-1.2.R,06:46:00,06:46:00,de:09179:6250:1:2,1,0,0,
#
# from LU
# trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled,timepoint
# 3:RGTR--:8378,19:23:00,19:23:00,190101018,1,,0,0,,1
# 3:RGTR--:8378,19:23:00,19:23:00,190101003,2,,0,0,,1
#


sub read_stop_times {
    my $filename        = shift;

    my $trip_id         = undef;
    my $departure       = undef;
    my $stop_id         = undef;
    my $stop_sequence   = undef;
    my @cells           = ();

    if ( open(STOPTIMES,$filename) ) {
        binmode STOPTIMES, ":utf8";

        while ( <STOPTIMES> ) {
            
            if ( m/^trip_id,/ ) {
                ;
            } else {
                @cells          = parse_csv( ',', $_ );
                $trip_id        = $cells[0];
                $departure      = $cells[2];
                $departure      =~ s/:\d\d$//;
                $stop_id        = $cells[3];
                $stop_sequence  = $cells[4];
                if ( defined $trip_id && defined $departure && defined $stop_id && defined $stop_sequence ) {
                    #printf STDERR "Trip-ID: %s - Stop-ID: %s - Stop-Sequence: %s\n", $trip_id, $stop_id, $stop_sequence     if ( $trip_id =~ m/-540-/ );
                    if ( $TRIPS{$trip_id}->{'stop_id'}->{$stop_id} ) {
                        $TRIPS{$trip_id}->{'stop_id'}->{$stop_id} .= ',' . $stop_sequence;
                    } else {
                        $TRIPS{$trip_id}->{'stop_id'}->{$stop_id}  = $stop_sequence;
                    }
                    if ( $TRIPS{$trip_id}->{'stop_id_list'} ) {
                        $TRIPS{$trip_id}->{'stop_id_list'}        .= ';' . $stop_id;
                    } else {
                        $TRIPS{$trip_id}->{'stop_id_list'}             = $stop_id;
                        $TRIPS{$trip_id}->{'stop_id_list_departure'}   = $departure;
                    }
                    #printf STDERR "Trip-ID: %s - Stop-id-list: %s\n", $trip_id, $TRIPS{$trip_id}->{'stop_id_list'}     if ( $trip_id =~ m/-540-3/ );
                    if ( $STOPS{$stop_id}->{'trip_id'}->{$trip_id} ) {
                        $STOPS{$stop_id}->{'trip_id'}->{$trip_id} .= ',' . $stop_sequence;
                    } else {
                        $STOPS{$stop_id}->{'trip_id'}->{$trip_id}  = $stop_sequence;
                    }
                } else {
                    printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
                }
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
# from DE-BY-MVV
# route_id,service_id,trip_id,trip_headsign
# 19-852-s19-1,19T0,68.T0.19-852-s19-1.7.R,Fürstenfeldbruck
#
# from LU
# route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed
# 3:RGTR--:215,7,3:RGTR--:8378,"Centre, Stäreplaz / Étoile",,1,,,,
# 3:RGTR--:215,7,3:RGTR--:8379,"Centre, Stäreplaz / Étoile",,1,,,,
#

sub read_trips {
    my $filename    = shift;

    my $route_id    = undef;
    my $trip_id     = undef;
    my @cells       = ();

    if ( open(TRIPS,$filename) ) {
        binmode TRIPS, ":utf8";

        while ( <TRIPS> ) {

            if ( m/^route_id,/ ) {
                ;
            } else {
                @cells      = parse_csv( ',', $_ );
                $route_id   = $cells[0];
                $trip_id    = $cells[2];
                if ( defined $route_id && defined $trip_id ) {
                    $ROUTES{$route_id}->{'trip_id'}->{$trip_id} = 1;
                    $TRIPS{$trip_id}->{'route_id'}->{$route_id} = 1;
                } else {
                    printf STDERR "Failed to scan line %d in %s: %s", $., $filename, $_;
                }
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
                # printf STDERR "stop_id = %s, route_id = %s route_short_name = %s\n", $stop_id, $route_id, $ROUTES{$route_id}->{'route_short_name'};
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
            printf "    <name><![CDATA[%s (Busses ~ '%s') @ %s%s]]></name>\r\n", $STOPS{$stop_id}->{'name'}, join(';',sort(keys(%{$STOPS{$stop_id}->{'route_short_names'}}))), $stop_id_is, $stop_id;
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


#############################################################################################
#
# return a list (array) fields of the CSV line
#
# https://stackoverflow.com/questions/3065095/how-do-i-efficiently-parse-a-csv-file-in-perl
#
#############################################################################################

sub parse_csv {
    my $separator = shift;
    my $text      = shift;
    my $value     = undef;
    my @cells     = ();
    my $regex     = qr/(?:^|$separator)(?:"([^"]*)"|([^$separator]*))/;

    return () unless $text;

    $text =~ s/\r?\n$//;

    while( $text =~ /$regex/g ) {
        $value = defined $1 ? $1 : $2;
        push( @cells, (defined $value ? $value : '') );
    }

    return @cells;
}




