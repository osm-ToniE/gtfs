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

my %STOP_LISTS_OF_ROUTE_SHORT_NAME  = ();   # key: short name of route, e.g. '210' or '975'

#$STOP_LISTS_OF_ROUTE_SHORT_NAME{'210'}->{'de:09162:1010:5:5|gen:9184:2302:0:3|gen:9184:2189:0:2'}->{'departures'}->{'08:15'} = 1;
#$STOP_LISTS_OF_ROUTE_SHORT_NAME{'210'}->{'de:09162:1010:5:5|gen:9184:2302:0:3|gen:9184:2189:0:2'}->{'departures'}->{'09:15'} = 1;
#$STOP_LISTS_OF_ROUTE_SHORT_NAME{'210'}->{'de:09162:1010:5:5|gen:9184:2302:0:3|gen:9184:2189:0:2'}->{'notes'}->{'subroute-of'}->{'02'} = 1;
#$STOP_LISTS_OF_ROUTE_SHORT_NAME{'210'}->{'de:09162:1010:5:5|gen:9184:2302:0:3|gen:9184:2189:0:2'}->{'notes'}->{'subroute-of'}->{'05'} = 1;
#$STOP_LISTS_OF_ROUTE_SHORT_NAME{'210'}->{'de:09162:1010:5:5|gen:9184:2302:0:3|gen:9184:2189:0:2'}->{'number'}                         = "01";
#$STOP_LISTS_OF_ROUTE_SHORT_NAME{'210'}->{'de:09162:1010:5:5|gen:9184:2302:0:3|gen:9184:2134:0:1|gen:9184:2134:0:2'}->{'notes'}->{'strange-end'} = 1;

#############################################################################################

use Getopt::Long;

my $verbose                         = undef;
my $debug                           = undef;
my $filename_routes                 = "routes.txt";
my $filename_stops                  = "stops.txt";
my $filename_stop_times             = "stop_times.txt";
my $filename_trips                  = "trips.txt";
my $outfile_prefix                  = undef;
my $route                           = undef;
my $all_routes                      = undef;

GetOptions( 'debug'                             =>  \$debug,                        # --debug
            'verbose'                           =>  \$verbose,                      # --verbose
            'routes=s'                          =>  \$filename_routes,              # --routes=
            'stops=s'                           =>  \$filename_stops,               # --stops=
            'stop-times=s'                      =>  \$filename_stop_times,          # --stop-times=
            'trips=s'                           =>  \$filename_trips,               # --trips=
            'prefix=s'                          =>  \$outfile_prefix,               # --prefix=                          --prefix="Bus"
            'route=s'                           =>  \$route,                        # --route=<route_short_name>         --route="210"
            'all-routes'                        =>  \$all_routes,                   # --all-routes                       overrules --route=
          );


#############################################################################################

if ( (!$route && !$all_routes) || !$outfile_prefix ) {
    printf STDERR "usage: gtfs-to-bus-routes-gpx.pl --route=<route_ref> --prefix=<prefix for out-files>\n";
    printf STDERR "example: gtfs-to-bus-routes-gpx.pl --route=210 --prefix=Busse/Bus\n";
    exit 1;
}

read_routes( $filename_routes );

read_stops( $filename_stops );

read_stop_times( $filename_stop_times );

read_trips( $filename_trips );

enhance_stops();

enhance_stop_times();

analyze_stop_lists();

if ( $all_routes ) {

    write_trips_of_route( '__all_routes__', $outfile_prefix );

} else {

    if ( $ROUTE_SHORT_NAME{$route} ) {
    
        write_trips_of_route( $route, $outfile_prefix );
        
    } else {
        printf STDERR "Route not found: %s\n", $route;
    }
}


#############################################################################################
#
# convert data GTFS routes.txt file to an input file for JOSM (.gpx)
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
# convert data GTFS stops.txt file to an input file for JOSM (.gpx)
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
# convert data GTFS stop_times.txt file to an input file for JOSM (.gpx)
#
# trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,timepoint
# 72.T0.19-852-s19-1.2.R,06:46:00,06:46:00,de:09179:6250:1:2,1,0,0,
#

sub read_stop_times {
    my $filename    = shift;
    
    my $trip_id         = undef;
    my $departure       = undef;
    my $stop_id         = undef;
    my $stop_sequence   = undef;

    if ( open(STOPTIMES,$filename) ) {
        binmode STOPTIMES, ":utf8";

        while ( <STOPTIMES> ) {
            
            if ( m/^trip_id,/ ) {
                ;
            } elsif ( m/^(.*?),(.*?),(.*?):\d\d,(.*?),(.*?),/ ) {
                $trip_id        = $1;
                $departure      = $3;
                $stop_id        = $4;
                $stop_sequence  = $5;
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
        close( STOPTIMES );
    } else {
        printf STDERR "Failed to open 'stop_times' file: %s\n", $filename;
    }
}


#############################################################################################
#
# convert data GTFS trips.txt file to an input file for JOSM (.gpx)
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

sub enhance_stop_times {
    my $route_id;
    my $stop_list;
    
    foreach my $route_short_name (  keys ( %ROUTE_SHORT_NAME ) ) {
        #printf STDERR "enhance_stop_times(): route_short_name = %s\n", $route_short_name;

        if ( $ROUTE_SHORT_NAME{$route_short_name}->{'route_id'} ) {
            
            $route_id = $ROUTE_SHORT_NAME{$route_short_name}->{'route_id'};
            
            #printf STDERR "Route: %s - Route-ID: %s\n", $route_short_name, $route_id     if ( $route_short_name eq $route );

            foreach my $trip_id ( keys( %{$ROUTES{$route_id}->{'trip_id'}} ) ) {

                #printf STDERR "Route: %s - Route-ID: %s - Trip-ID: %s\n", $route_short_name, $route_id,$trip_id     if ( $route_short_name eq $route );

                $stop_list = $TRIPS{$trip_id}->{'stop_id_list'};
                
                if ( $stop_list ) {
                    $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_list}->{'departures'}->{$TRIPS{$trip_id}->{'stop_id_list_departure'}} = 1;
                } else {
                    printf STDERR "enhance_stop_times(): no stop_list for Route: %s, Trip: %s\n", $route_short_name, $trip_id;
                }
            }
        }
    }
}


#############################################################################################
#
#
#

sub analyze_stop_lists {
    my $route_short_name        = undef;
    my $stop_list               = undef;
    my @stop_lists              = ();
    my $index1                  = 0;
    my $index2                  = 0;
    my $num_of_stop_lists       = 0;
    my $last_stop               = undef;
    my $second_last_stop        = undef;
    my @last_stop_split         = ();
    my @second_last_stop_split  = ();
    my $new_stop_list           = undef;
    
    foreach my $route_short_name (  keys ( %STOP_LISTS_OF_ROUTE_SHORT_NAME ) ) {

        # printf STDERR "analyze_stop_lists(): route_short_name = %s\n", $route_short_name;
        
        # check for strange end of list, where the last stop is the second-last stop but in opposite direction or so
        
        @stop_lists = sort( keys( %{$STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}} ) );
        
        $num_of_stop_lists = scalar( @stop_lists );
        
        for ( $index1 = 0; $index1 < $num_of_stop_lists; $index1++ ) {
            if ( $stop_lists[$index1] =~ m/;([^;]+);([^;]+)$/ ) {
                $second_last_stop   = $1;
                $last_stop          = $2;
                
                @last_stop_split        = split( ':', $last_stop );
                @second_last_stop_split = split( ':', $second_last_stop );
                
                if ( defined($last_stop_split[0]) && defined($second_last_stop_split[0]) && $last_stop_split[0] eq $second_last_stop_split[0] &&
                     defined($last_stop_split[1]) && defined($second_last_stop_split[1]) && $last_stop_split[1] eq $second_last_stop_split[1] &&
                     defined($last_stop_split[2]) && defined($second_last_stop_split[2]) && $last_stop_split[2] eq $second_last_stop_split[2]    ) {
                    
                    #printf STDERR "Strange End for %s - %d: %s ~ %s\n", $route_short_name, $index1+1, $second_last_stop, $last_stop;
                    $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_lists[$index1]}->{'notes'}->{'strange-end'} = 1;
                    
                    $new_stop_list = $stop_lists[$index1]; 
                    $new_stop_list =~ s/;([^;]+)$//;
                    
                    foreach my $departures ( keys ( %{$STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_lists[$index1]}->{'departures'}} ) ) {
                        $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$new_stop_list}->{'departures'}->{$departures} = 1;
                    }
                }
            }
        }

        # we might have added new routes in the step above, so let's rearrange ...
        # check for subroutes
        
        @stop_lists = sort( keys( %{$STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}} ) );
        
        $num_of_stop_lists = scalar( @stop_lists );
        
        for ( $index1 = 0; $index1 < $num_of_stop_lists; $index1++ ) {
            
            $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_lists[$index1]}->{'number'} = sprintf("%02d",$index1+1);

            for ( $index2 = 0; $index2 < $num_of_stop_lists; $index2++ ) {

                next    if ( $index1 == $index2 );
                
                next    if ( $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_lists[$index1]}->{'notes'}->{'strange-end'} ||
                             $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_lists[$index2]}->{'notes'}->{'strange-end'}    );
                
                #printf STDERR "Analyze %s\n %d   %s\n %d   %s\n", $route_short_name, $index1+1, $stop_lists[$index1], $index2+1, $stop_lists[$index2] if ( $route_short_name eq '210' );
                if ( $stop_lists[$index2] =~ m/\Q$stop_lists[$index1]\E/ ) {
                    #printf STDERR "%d is subroute of %d\n", $index1+1, $index2+1  if ( $route_short_name eq '210' );
                    $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_lists[$index1]}->{'notes'}->{'subroute-of'}->{sprintf("%02d",$index2+1)} = 1;
                }
            }
        }
    }
}


#############################################################################################
#
#
#

sub write_trips_of_route {
    my $route_short_name = shift;
    my $prefix           = shift;

    my $filename    = undef;
    my $pretty_route= undef;
    my $departures  = '';
    my @routes      = ();
    my $list_ptr    = undef;
    my $index       = 0;
    my $help        = undef;
    
    if ( $route_short_name && $prefix ) {
        
        if ( $route_short_name eq '__all_routes__' ) {
            @routes = sort ( keys( %STOP_LISTS_OF_ROUTE_SHORT_NAME ) );
        } else {
            push( @routes, $route_short_name );
        }
        
        foreach $route_short_name ( @routes ) {
            
            printf STDERR "%s\n", $route_short_name     if ( $verbose );
            
            foreach my $stop_list ( keys( %{$STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}} ) ) {
                
                $list_ptr = $STOP_LISTS_OF_ROUTE_SHORT_NAME{$route_short_name}->{$stop_list};
                
                $pretty_route = $route_short_name;
                $pretty_route =~ s/[^A-Za-z0-9_-]/_/g;
                
                $help = undef;
                if ( $list_ptr->{'notes'}->{'strange-end'} ) {
                    $filename = $prefix . '-' . $pretty_route . '-' . $list_ptr->{'number'} . '-strange-end' . '.gpx';
                } else {
                    $help = join( '-and-', sort ( keys (%{$list_ptr->{'notes'}->{'subroute-of'}} ) ) );
                    if ( $help ) {
                        $filename = $prefix . '-' . $pretty_route . '-' . $list_ptr->{'number'} . '-subroute-of-' . $help . '.gpx';
                    } else {
                        $filename = $prefix . '-' . $pretty_route . '-' . $list_ptr->{'number'} . '.gpx';
                    }
                }
                
                
                $departures = join( ',', sort( keys( %{$list_ptr->{'departures'}} ) ) );
                
                #printf STDERR "creating %s\n", $filename;
                
                if ( open(ROUTE,"> $filename") ) {
                    binmode ROUTE, ":utf8";
        
                    printf ROUTE "<?xml version='1.0' encoding='UTF-8' ?>\r\n";
                    printf ROUTE "<gpx xmlns='http://www.topografix.com/GPX/1/1' version='1.0' creator='gtfs-to-bus-routes-gpx.pl' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd'>\r\n";
                    #
                    # write the way points
                    #
                    $index = 0;
                    foreach my $stop_id ( split( ';', $stop_list ) ) {
                        $index++;
                        if ( $STOPS{$stop_id}->{'name'} && $STOPS{$stop_id}->{'lat'} && $STOPS{$stop_id}->{'lon'} ) {
                            printf ROUTE "    <wpt lat='%s' lon='%s'>\r\n", $STOPS{$stop_id}->{'lat'}, $STOPS{$stop_id}->{'lon'};
                            if ( $index == 1 ) {
                                printf ROUTE "        <name><![CDATA[#%s %s (IFOPT = %s) @ %s]]></name>\r\n", $index, $STOPS{$stop_id}->{'name'}, $stop_id, $departures;
                            } else {
                                printf ROUTE "        <name><![CDATA[#%s %s (IFOPT = %s)]]></name>\r\n", $index, $STOPS{$stop_id}->{'name'}, $stop_id;
                            }
                            printf ROUTE "    </wpt>\r\n";
                        } else {
                            printf STDERR "Missing information for STOP_ID: %s\n", $stop_id;
                            foreach my $key ( sort ( keys ( %{$STOPS{$stop_id}} ) ) ) {
                                printf STDERR "    \$STOPS{%s}->{%s} = %s\n", $stop_id, $key, $STOPS{$stop_id}->{$key};
                            }
                            printf STDERR "\n";
                        }
                    }
                    printf ROUTE "    <rte>\r\n";
                    printf ROUTE "        <name><![CDATA[%s: departures %s]]></name>\r\n", $route_short_name, $departures;
                    $index = 0;
                    foreach my $stop_id ( split( '\|', $stop_list ) ) {
                        $index++;
                        if ( $STOPS{$stop_id}->{'name'} && $STOPS{$stop_id}->{'lat'} && $STOPS{$stop_id}->{'lon'} ) {
                            printf ROUTE "        <rtept lat='%s' lon='%s' />\r\n", $STOPS{$stop_id}->{'lat'}, $STOPS{$stop_id}->{'lon'};
                        }
                    }
                    printf ROUTE "    </rte>\r\n";
                    printf ROUTE "</gpx>\r\n";
                    
                    close( ROUTE );
                } else {
                    printf STDERR "Can't create file: %s\n", $filename;
                }
            }
        }
    } else {
        printf STDERR "write_trips_of_route(): \$route_short_name not defined\n" if ( !$route_short_name );
        printf STDERR "write_trips_of_route(): \$prefix           not defined\n" if ( !$prefix           );
    }

}

