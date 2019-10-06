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

use DBI;


#############################################################################################

use Getopt::Long;

my $debug                   = undef;
my $verbose                 = undef;
my $new_dir                 = undef;
my $new_name                = undef;
my $old_dir                 = undef;
my $old_name                = undef;
my $list_this               = 'stops';
my $format                  = undef;
my $print_all               = undef;

my $errors                  = 0;

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'new-dir=s'             =>  \$new_dir,               # --new-dir=
            'old-dir=s'             =>  \$old_dir,               # --old-dir=
            'new-name=s'            =>  \$new_name,              # --new-name=
            'old-name=s'            =>  \$old_name,              # --old-name=
            'list=s'                =>  \$list_this,             # --list=           --list=stops / --list=routes / --list=trips / --list=agency
            'format=s'              =>  \$format,                # --format=html
            'print-all'             =>  \$print_all,             # --print-all
          );


#############################################################################################

if ( $old_dir ) {
    if ( -d $old_dir ) {
        printf STDERR "Evaluating: %s/%s.txt\n", $old_dir, $list_this       if ( $verbose );
        if ( -f "$old_dir/$list_this.txt" && -r "$old_dir/$list_this.txt" ) {
        } else {
            $errors++;
        }
    } else {
        $errors++;
    }
} else {
    $errors++;
}

if ( $new_dir ) {
    if ( -d $new_dir ) {
        printf STDERR "Evaluating: %s/%s.txt\n", $new_dir, $list_this       if ( $verbose );
        if ( -f "$new_dir/$list_this.txt" && -r "$new_dir/$list_this.txt" ) {
        } else {
            $errors++;
        }
    } else {
        $errors++;
    }
} else {
    $errors++;
}

if ( $format ) {

    if ( $format !~ m/^html$/i ) {
        printf STDERR "--format=%s - allowed value: 'HTML'\n", $format;
        $format = undef;
    }
}

if ( $errors ) {
    printf STDERR "%s --list=agency --format=html --old-dir=xxx --new-dir=yyy\n", $0;
    printf STDERR "%s --list=routes --format=html --old-dir=xxx --new-dir=yyy\n", $0;
    printf STDERR "%s --list=stops  --format=html --old-dir=xxx --new-dir=yyy\n",  $0;
    printf STDERR "%s --list=trips  --format=html --old-dir=xxx --new-dir=yyy\n",  $0;
    
    exit 1;
}


$new_name = $new_dir    unless ( $new_name );
$old_name = $old_dir    unless ( $old_name );

#############################################################################################
#
#
#

my $old_dbh = DBI->connect( "dbi:CSV:f_dir=$old_dir;csv_sep_char=,", "", "", { AutoCommit=> 1, RaiseError => 1 } )  or die "Connect to DB for old GTFS info failed";

my $new_dbh = DBI->connect( "dbi:CSV:f_dir=$new_dir;csv_sep_char=,", "", "", { AutoCommit=> 1, RaiseError => 1 } )  or die "Connect to DB for new GTFS info failed";

my @results = ();


printf STDERR "Evaluating: %s\n", $list_this    if ( $verbose );

if ( $list_this eq 'agency'  ) {

    check_agency();

} elsif ( $list_this eq 'routes' ) {

    check_routes();

} elsif ( $list_this eq 'stops' ) {

    @results = check_stops();
    
    if ( $format && $format =~ m/^html$/ ) {
        print_stops_html( 'results-ref' => \@results, 'old-name' => $old_name, 'new-name' => $new_name );
    } else {
        foreach my $line ( @results ) {
            printf STDOUT "%s\n", $line;
        }
    }

}  elsif ( $list_this eq 'trips'  ) {

    check_trips();

}

$old_dbh->disconnect();

$new_dbh->disconnect();


#############################################################################################
#
#
#

sub check_agency {
    my $sth                 = undef;
    my @row                 = ();

    my %check_hash          = ();
    my $key                 = undef;
    my $columns             = 'agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url';
    my $agency_id_index     = 0;
    my $agency_name_index   = 1;
    my $agency_url_index    = 2;
    my $agency_timezone     = 3;
    my $agency_lang         = 4;
    my $agency_phone        = 5;
    my $agency_fare_url     = 6;
    my $key_index           = $agency_name_index;
    my $old_new             = undef;

    $old_new = 'old';
    $sth = $old_dbh->prepare( "SELECT $columns FROM agency.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;

        $check_hash{$key}->{$old_new} = 1;

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }

    $old_new = 'new';
    $sth = $new_dbh->prepare( "SELECT $columns FROM agency.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;

        $check_hash{$key}->{$old_new} = 1;

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }
    
    foreach $key ( sort ( keys ( %check_hash ) ) ) {
        if ( exists($check_hash{$key}->{'old'}) && exists($check_hash{$key}->{'new'}) ) {
            printf STDOUT "%s,1,0\n", $key    if ( $debug );
            ; # fine, let's check for more diffs
        } elsif ( exists($check_hash{$key}->{'old'}) ) {
            printf STDOUT "%s,1,0\n", $key;
        } elsif ( exists($check_hash{$key}->{'new'}) ) {
            printf STDOUT "%s,0,1\n", $key;
        } else {
            printf STDERR "Internal error: neither 'old' nor 'new' do exist for agency '%s'\n", $key;
        }
    }

}


#############################################################################################
#
#
#

sub check_routes {
    my $sth                     = undef;
    my @row                     = ();

    my %check_hash              = ();
    my $key                     = undef;
    my $columns                 = 'route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color';
    my $route_id_index          = 0;
    my $agency_id_index         = 1;
    my $route_short_name_index  = 2;
    my $route_long_name_index   = 3;
    my $route_type_index        = 4;
    my $route_url_index         = 5;
    my $route_color_index       = 6;
    my $route_text_color_index  = 7;
    my $key_index               = $route_short_name_index;
    my $old_new                 = undef;

    $old_new = 'old';
    $sth = $old_dbh->prepare( "SELECT $columns FROM routes.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;

        $check_hash{$key}->{$old_new} = 1;

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }

    $old_new = 'new';
    $sth = $new_dbh->prepare( "SELECT $columns FROM routes.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;

        $check_hash{$key}->{$old_new} = 1;

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }

    foreach $key ( sort ( keys ( %check_hash ) ) ) {
        if ( exists($check_hash{$key}->{'old'}) && exists($check_hash{$key}->{'new'}) ) {
            printf STDOUT "%s,1,0\n", $key    if ( $debug );
            ; # fine, let's check for more diffs
        } elsif ( exists($check_hash{$key}->{'old'}) ) {
            printf STDOUT "%s,1,0\n", $key;
        } elsif ( exists($check_hash{$key}->{'new'}) ) {
            printf STDOUT "%s,0,1\n", $key;
        } else {
            printf STDERR "Internal error: neither 'old' nor 'new' do exist for route '%s'\n", $key;
        }
    }

}


#############################################################################################
#
#
#

sub check_stops {
    my $sth                     = undef;
    my @row                     = ();

    my %check_hash              = ();
    my %found_stop_ids          = ();
    my $key                     = undef;
    my $key2                    = undef;
    my $columns                 = 'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,platform_code,stop_url';
    my $stop_id_index           = 0;
    my $stop_name_index         = 1;
    my $stop_lat_index          = 2;
    my $stop_lon_index          = 3;
    my $location_type_index     = 4;
    my $parent_station_index    = 5;
    my $platform_code_index     = 6;
    my $stop_url_index          = 7;
    my $key_index               = $stop_name_index;
    my $key2_index              = $stop_id_index;
    my $old_new                 = undef;
    my $result_line             = undef;
    my @result_lines            = ();
    my $result_diffs            = 0;

    $old_new = 'old';
    $sth = $old_dbh->prepare( "SELECT $columns FROM stops.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {
        
        $key =  $row[$key_index];
        $key =~ s/Str\./Straße/;
        $key =~ s/str\./straße/;
        $key =~ s/Str$/Straße/;
        $key =~ s/str$/straße/;
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;
        $key =~ s/Fürstenfeldbr./Fürstenfeldbruck/;
        $key =~ s/Freising P\+R-Platz/Freising *300 P+R-Platz/;
        $key =~ s/Petershausen P\+R-Platz/Petershausen*300 P+R-Platz/;
        $key =~ s/Realschule Gute Änger/RS Gute Änger/;
        $key =~ s/Oberallershausen, Schroßlacher/Oberallershausen, Schroßlach/;
        $key =~ s/Gelting, Geltingerau/Gelting, Geltinger Au/;
        $key2 = $row[$key2_index];
        $key2 =~ s/^gen://;
        $key2 =~ s/^de:0//;

        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_id'}         = $row[$stop_id_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_name'}       = $row[$stop_name_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_lat'}        = $row[$stop_lat_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_lon'}        = $row[$stop_lon_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'location_type'}   = $row[$location_type_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'parent_station'}  = $row[$parent_station_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'platform_code'}   = $row[$platform_code_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_url'}        = $row[$stop_url_index];

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }

    $old_new = 'new';
    $sth = $new_dbh->prepare( "SELECT $columns FROM stops.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/Str\./Straße/;
        $key =~ s/str\./straße/;
        $key =~ s/Str$/Straße/;
        $key =~ s/str$/straße/;
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;
        $key =~ s/Fürstenfeldbr./Fürstenfeldbruck/;
        $key =~ s/Freising P\+R-Platz/Freising *300 P+R-Platz/;
        $key =~ s/Petershausen P\+R-Platz/Petershausen*300 P+R-Platz/;
        $key =~ s/Realschule Gute Änger/RS Gute Änger/;
        $key =~ s/Oberallershausen, Schroßlacher/Oberallershausen, Schroßlach/;
        $key =~ s/Gelting, Geltingerau/Gelting, Geltinger Au/;
        $key2 = $row[$key2_index];
        $key2 =~ s/^gen://;
        $key2 =~ s/^de:0//;

        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_id'}         = $row[$stop_id_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_name'}       = $row[$stop_name_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_lat'}        = $row[$stop_lat_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_lon'}        = $row[$stop_lon_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'location_type'}   = $row[$location_type_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'parent_station'}  = $row[$parent_station_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'platform_code'}   = $row[$platform_code_index];
        $check_hash{$key}->{$old_new}->{'stop_id'}->{$key2}->{'stop_url'}        = $row[$stop_url_index];

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }

    foreach $key ( sort ( keys ( %check_hash ) ) ) {
        if ( exists($check_hash{$key}->{'old'}->{'stop_id'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}) ) {
            
            %found_stop_ids = ();
            
            map { $found_stop_ids{$_} = 1; } keys ( %{$check_hash{$key}->{'old'}->{'stop_id'}} );
            map { $found_stop_ids{$_} = 1; } keys ( %{$check_hash{$key}->{'new'}->{'stop_id'}} );
            
            foreach my $stop_id ( sort ( keys ( %found_stop_ids ) ) ) {

                $result_diffs = 0;
                $result_line  = sprintf "\"%s\",", $key;
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'};
                } elsif ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'}) ) {
                    $result_line .= sprintf "D:\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'};
                } elsif ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'}) ) {
                    $result_line .= sprintf "D:\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'};
                } elsif ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'}) ) {
                    $result_line .= sprintf "D:\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'};
                } elsif ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'}) ) {
                    $result_line .= sprintf "D:\"%s\",", $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_id'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'};
                } elsif ( exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'}) ) {
                    $result_line .= sprintf "N:\"%s\",", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_id'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_name'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'};
                } elsif ( exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'}) ) {
                    $result_line .= sprintf "N:\"%s\",", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_name'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lat'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\",", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'};
                } elsif ( exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'}) ) {
                    $result_line .= sprintf "N:\"%s\",", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lat'},
                    $result_diffs++;
                } else {
                    $result_line .= sprintf ",";
                }
                if ( exists($check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'}) && exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'}) ) {
                    if ( $check_hash{$key}->{'old'}->{'stop_id'}->{$stop_id}->{'stop_lon'} ne $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'} ) {
                        $result_line .= sprintf "C:";
                        $result_diffs++;
                    }
                    $result_line .= sprintf "\"%s\"", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'};
                } elsif ( exists($check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'}) ) {
                    $result_line .= sprintf "N:\"%s\"", $check_hash{$key}->{'new'}->{'stop_id'}->{$stop_id}->{'stop_lon'},
                    $result_diffs++;
                }
                
                push( @result_lines, $result_line )  if ( $print_all || $result_diffs );
            }
        } elsif ( exists($check_hash{$key}->{'old'}->{'stop_id'}) ) {
            $old_new = 'old';
            foreach my $stop_id ( sort ( keys ( %{$check_hash{$key}->{$old_new}->{'stop_id'}} ) ) ) {
                $result_line = sprintf "D:\"%s\",D:\"%s\",D:\"%s\",D:\"%s\",D:\"%s\",,,,",
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_name'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_id'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_name'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_lat'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_lon'};
                push( @result_lines, $result_line );
            }
        } elsif ( exists($check_hash{$key}->{'new'}->{'stop_id'}) ) {
            $old_new = 'new';
            foreach my $stop_id ( sort ( keys ( %{$check_hash{$key}->{$old_new}->{'stop_id'}} ) ) ) {
                $result_line = sprintf "N:\"%s\",,,,,N:\"%s\",N:\"%s\",N:\"%s\",N:\"%s\"",
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_name'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_id'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_name'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_lat'},
                                        $check_hash{$key}->{$old_new}->{'stop_id'}->{$stop_id}->{'stop_lon'};
                push( @result_lines, $result_line );
            }
        } else {
            printf STDERR "Internal error: neither 'old' nor 'new' do exist for stop '%s'\n", $key;
        }
    }

    return @result_lines;
    
}


#############################################################################################
#
#
#

sub print_stops_html {
    my %hash             = ( @_ );
    my $result_array_ref = $hash{'results-ref'};
    my $old_name         = $hash{'old-name'} || 'old';
    my $new_name         = $hash{'new-name'} || 'new';
    
    print_html_header( 'Stops' );
    
    print_html_table_header( '&nbsp;', "C4:GTFS ($old_name)", "C4:GTFS ($new_name)" );
    
    print_html_table_subheader( 'Name', 'IFOPT', 'Name', 'Lat', 'Lon', 'IFOPT', 'Name', 'Lat', 'Lon' );
    
    if ( $result_array_ref ) {
        foreach my $line ( @{$result_array_ref} ) {
            $line =~ s|^C:"|                <tr><td class=\"changed\">|;
            $line =~ s|^D:"|                <tr><td class=\"deleted\">|;
            $line =~ s|^N:"|                <tr><td class=\"new\">|;
            $line =~ s|^"|                <tr><td>|;
            $line =~ s|",C:"|</td><td class=\"changed\">|g;
            $line =~ s|",D:"|</td><td class=\"deleted\">|g;
            $line =~ s|",N:"|</td><td class=\"new\">|g;
            $line =~ s|,C:"|,</td><td class=\"changed\">|g;
            $line =~ s|,D:"|,</td><td class=\"deleted\">|g;
            $line =~ s|,N:"|,</td><td class=\"new\">|g;
            $line =~ s|","|</td><td>|g;
            $line =~ s|"$|</td></tr>|;
            $line =~ s|",,,,,|</td><td></td><td></td><td></td><td>|;
            $line =~ s|",,,,$|</td><td></td><td></td><td></td><td>|;
            printf STDOUT "%s\r\n", $line;
        }
    }
    
    print_html_table_footer();
    
    print_html_footer();
}


#############################################################################################
#
#
#

sub check_trips {
    my $sth                 = undef;
    my @row                 = ();

    my %check_hash          = ();
    my $key                 = undef;
    my $columns             = 'route_id,service_id,trip_id,trip_headsign';
    my $route_id_index      = 0;
    my $service_id_index    = 1;
    my $trip_id_index       = 2;
    my $trip_headsign_index = 3;
    my $key_index           = $trip_headsign_index;
    my $old_new             = undef;

    $old_new = 'old';
    $sth = $old_dbh->prepare( "SELECT $columns FROM trips.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/Str\./Straße/;
        $key =~ s/str\./straße/;
        $key =~ s/Str$/Straße/;
        $key =~ s/str$/straße/;
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;
        $key =~ s/Fürstenfeldbr./Fürstenfeldbruck/;
        $key =~ s/Freising P\+R-Platz/Freising *300 P+R-Platz/;
        $key =~ s/Petershausen P\+R-Platz/Petershausen*300 P+R-Platz/;
        $key =~ s/Realschule Gute Änger/RS Gute Änger/;
        $key =~ s/Oberallershausen, Schroßlacher/Oberallershausen, Schroßlach/;
        $key =~ s/Gelting, Geltingerau/Gelting, Geltinger Au/;

        $check_hash{$key}->{$old_new} = 1;

        printf STDERR "%s: %s\n", $old_new,$key    if ( $verbose );
    }

    $old_new = 'new';
    $sth = $new_dbh->prepare( "SELECT $columns FROM trips.txt" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        $key =  $row[$key_index];
        $key =~ s/Str\./Straße/;
        $key =~ s/str\./straße/;
        $key =~ s/Str$/Straße/;
        $key =~ s/str$/straße/;
        $key =~ s/,/, /g;
        $key =~ s/\s+/ /g;
        $key =~ s/Fürstenfeldbr./Fürstenfeldbruck/;
        $key =~ s/Freising P\+R-Platz/Freising *300 P+R-Platz/;
        $key =~ s/Petershausen P\+R-Platz/Petershausen*300 P+R-Platz/;
        $key =~ s/Realschule Gute Änger/RS Gute Änger/;
        $key =~ s/Oberallershausen, Schroßlacher/Oberallershausen, Schroßlach/;
        $key =~ s/Gelting, Geltingerau/Gelting, Geltinger Au/;

        $check_hash{$key}->{$old_new} = 1;

        printf STDERR "%s: %s\n", $old_new, $key    if ( $verbose );
    }

    foreach $key ( sort ( keys ( %check_hash ) ) ) {
        if ( exists($check_hash{$key}->{'old'}) && exists($check_hash{$key}->{'new'}) ) {
            printf STDOUT "%s,1,0\n", $key    if ( $debug );
            ; # fine, let's check for more diffs
        } elsif ( exists($check_hash{$key}->{'old'}) ) {
            printf STDOUT "%s,1,0\n", $key;
        } elsif ( exists($check_hash{$key}->{'new'}) ) {
            printf STDOUT "%s,0,1\n", $key;
        } else {
            printf STDERR "Internal error: neither 'old' nor 'new' do exist for trip '%s'\n", $key;
        }
    }

}


#############################################################################################
#
#
#

sub print_html_header {
    my $what = shift || '';

    printf STDOUT "<!DOCTYPE html>\r\n";
    printf STDOUT "<html lang=\"de\">\r\n";
    printf STDOUT "    <head>\r\n";
    printf STDOUT "        <title>GTFS %s Diff</title>\r\n", $what;
    printf STDOUT "        <meta charset=\"utf-8\" />\r\n";
    printf STDOUT "        <meta name=\"generator\" content=\"gtfs-diff\">\r\n";
    printf STDOUT "        <meta name=\"keywords\" content=\"GTFS\">\r\n";
    printf STDOUT "        <meta name=\"description\" content=\"GTFS %s diff\">\r\n", $what;
    printf STDOUT "        <style>\r\n";
    printf STDOUT "              table { border-width: 1px; border-style: solid; border-collapse: collapse; vertical-align: center; }\r\n";
    printf STDOUT "              th    { border-width: 1px; border-style: solid; border-collapse: collapse; padding: 0.2em; }\r\n";
    printf STDOUT "              td    { border-width: 1px; border-style: solid; border-collapse: collapse; padding: 0.2em; }\r\n";
    printf STDOUT "              .tableheaderrow    { background-color: LightSteelBlue;   }\r\n";
    printf STDOUT "              .tableheadersubrow { background-color: LightBlue;        }\r\n";
    printf STDOUT "              .deleted           { color: black; background-color: Orange;      }\r\n";
    printf STDOUT "              .new               { color: black; background-color: SpringGreen; }\r\n";
    printf STDOUT "              .changed           { color: black; background-color: GreenYellow; }\r\n";
    printf STDOUT "        </style>\r\n";
    printf STDOUT "    </head>\r\n";
    printf STDOUT "    <body>\r\n";

}


#############################################################################################
#
#
#

sub print_html_footer {
    
    printf STDOUT "    </body>\r\n";
    printf STDOUT "</html>\r\n";

}


#############################################################################################
#
#
#

sub print_html_table_header {
    my @columns = ( @_ );

    printf STDOUT "%8s<table class=\"gtfsdifftable\">\r\n", ' ';
    printf STDOUT "%12s<thead>\r\n", ' ';
    printf STDOUT "%16s<tr class=\"tableheaderrow\">", ' ';
    foreach my $col ( @columns ) {
        if ( $col =~ m/^C(\d):(.*)$/ ) {
            printf STDOUT "<th colspan=\"%d\">%s</th>", $1, $2;
        } else {
            printf STDOUT "<th>%s</th>", $col;
        }
    }
    printf STDOUT "</tr>\r\n";
    printf STDOUT "%12s</thead>\r\n", ' ';
    printf STDOUT "%12s<tbody>\r\n", ' ';
}


#############################################################################################
#
#
#

sub print_html_table_subheader {
    my @columns = ( @_ );
    printf STDOUT "%16s<tr class=\"tableheadersubrow\">", ' ';
    foreach my $col ( @columns ) {
        printf STDOUT "<th>%s</th>", $col;
    }
    printf STDOUT "</tr>\r\n";
}


#############################################################################################
#
#
#

sub print_html_table_footer {
    printf STDOUT "%12s</tbody>\r\n", ' ';
    printf STDOUT "%8s</table>\r\n",  ' ';

}





