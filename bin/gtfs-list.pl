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

use Date::Calc;


#############################################################################################

my %route_type_2_id     = ( 'tram'      => '0',
                            'subway'    => '1',
                            'train'     => '2', 'light_rail' => '2', 'monorail'   => '2',
                            'bus'       => '3', 'trolleybus' => '3', 'share_taxi' => '3',
                            'ferry'     => '4',
                            'aerialway' => '6',
                            'funicular' => '7'
                          );

my %route_id_2_type     = ( '0' => 'tram',
                            '1' => 'subway',
                            '2' => 'train',
                            '3' => 'bus',
                            '4' => 'ferry',
                            '5' => 'tram',
                            '6' => 'aerialway',
                            '7' => 'funicular',
                          );

#############################################################################################

use Getopt::Long;

my $debug                    = undef;
my $verbose                  = undef;
my $ifopt_levels             = 5;
my $list_this                = undef;
my $format                   = undef;
my $agency                   = undef;
my $route_short_name         = undef;
my $route_type               = undef;
my $show_dates               = undef;
my $comment                  = 'route_id';

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'ifopt-levels=i'        =>  \$ifopt_levels,          # --ifopt-levels=   --ifopt-levels=1 / --ifopt-levels=2 / --ifopt-levels=3 / --ifopt-levels=4 / --ifopt-levels=5
            'list=s'                =>  \$list_this,             # --list=           --list=ifopt|IFOPT|stops / --list=routes / --list=agency / --list=agency-id
            'format=s'              =>  \$format,                # --format=PTNA
            'agency=s'              =>  \$agency,                # --agency=
            'route-short-name=s'    =>  \$route_short_name,      # --route-short-name=
            'route-type=s'          =>  \$route_type,            # --route-type=
            'show-dates'            =>  \$show_dates,            # --show-dates
            'comment=s'             =>  \$comment,               # --comment=route_id
          );


#############################################################################################
#
#
#

if ( $format ) {

    if ( $format !~ m/^ptna$/i ) {
        printf STDERR "--format=%s - allowed value: 'PTNA'\n", $format;
        $format = undef;
    }
}


#############################################################################################
#
#
#

my %service_ids = (); 

my %service_ids_of_route_ids = (); 


#############################################################################################
#
#
#

my $dbh = DBI->connect( "dbi:CSV:f_dir=.;f_ext=.txt;csv_sep_char=,", "", "", { AutoCommit=> 1, RaiseError => 1 } )  or die "Connect to DB failed";


if ( $list_this ) {

    if ( $list_this =~ m/^(ifopt|stops)$/i ) {

        list_stops( $list_this );

    } elsif ( $list_this eq 'routes' ) {

        if ( defined $agency  && $agency !~ m/^[0-9]+$/ ) {
           $agency = find_agency_id( $agency );
        }

        if ( defined $route_type && $route_type !~ m/^[0-9]+$/ ) {
            if ( defined $route_type_2_id{$route_type} ) {
                $route_type  =  $route_type_2_id{$route_type};
            } else {
                printf STDERR "unknown value for --route-type=%s\n", $route_type;
                $route_type  =  undef;
            }
        }
        
        if ( $show_dates ) {
            read_calendar();
            
            read_trips();
        }

        list_routes( format => $format, agency => $agency, route_type => $route_type, route_short_name => $route_short_name, comment => $comment, show_dates => $show_dates );

    }  elsif ( $list_this eq 'agency'  ) {

        list_agencies();

    }  elsif ( $list_this eq 'agency-id'  ) {

        list_agency_ids();

    }  elsif ( $list_this eq 'route-type'  ) {

        list_route_type();

    }
} else {

    printf STDERR "%s --list=ifopt [--ifopt-levels=1|2|3|4|5]\n", $0;
    printf STDERR "%s --list=stops\n", $0;
    printf STDERR "%s --list=routes [--format=PTNA][--agency=...][--route-type=...]\n", $0;
    printf STDERR "%s --list=agency\n", $0;
    printf STDERR "%s --list=agency-id\n", $0;
    printf STDERR "%s --list=route-type\n", $0;

}

$dbh->disconnect();


#############################################################################################
#
#
#

sub list_stops {
    my $list_this       = shift || 'stops';

    my %printed         = ();
    my @ifopt_parts     = ();
    my $ifopt_part      = undef;
    my $sth             = undef;
    my @row             = ();

    $sth = $dbh->prepare( "SELECT stop_id FROM stops" );
    $sth->execute();

    if ( $list_this eq 'stops' ) {

        while ( @row = $sth->fetchrow_array ) {

            printf STDOUT "%s\n", $row[0]      unless ( $printed{$row[0]} );

            $printed{$row[0]} = 1;
        }
    } else {
        while ( @row = $sth->fetchrow_array ) {

            if ( $ifopt_levels < 1 || $ifopt_levels > 4 ) {

                printf STDOUT "%s\n", $row[0];

            } else {

                @ifopt_parts = split( ':', $row[0] );
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

                    printf STDERR "Can't split %s\n", $row[0];
                }

            }
        }
    }

}


#############################################################################################
#
#
#

sub list_routes {
    my %hash                        = ( @_ );

    my $opt_format                  = $hash{'format'};
    my $opt_agency                  = $hash{'agency'};
    my $opt_route_type              = $hash{'route_type'};
    my $opt_route_short_name        = $hash{'route_short_name'};
    my $opt_comment                 = $hash{'comment'};
    my $route_comment               = undef;
    my $route_id                    = undef;
    my $route_short_name            = undef;
    my $route_long_name             = undef;
    my $route_type                  = undef;
    my $ptna_ref                    = undef;
    my $ptna_route_type             = undef;
    my $ptna_comment                = undef;
    my $ptna_from                   = undef;
    my $ptna_to                     = undef;
    my $ptna_operator               = '';
    my @clauses                     = ();
    my @bind                        = ();
    my $clause                      = '';
    my $sth                         = undef;
    my @row                         = ();

    if ( defined $opt_agency     )          { push(@clauses,"agency_id=?" );        push(@bind,$opt_agency    ); }
    if ( defined $opt_route_type )          { push(@clauses,"route_type=?");        push(@bind,$opt_route_type); }
    if ( defined $opt_route_short_name )    { push(@clauses,"route_short_name=?");  push(@bind,$opt_route_short_name); }

    if ( scalar @clauses ) {
        $clause = "WHERE " . join( " AND ", @clauses )
    }

    printf STDERR "SELECT %s,route_id,route_short_name,route_long_name,route_type FROM routes %s\n", $comment, $clause if ( $debug );

    $sth = $dbh->prepare( "SELECT $comment,route_id,route_short_name,route_long_name,route_type FROM routes $clause" );
    $sth->execute(@bind);

    while ( @row = $sth->fetchrow_array ) {

        ($route_comment,$route_id,$route_short_name,$route_long_name,$route_type) = @row;
        
        printf STDERR "comment=%s, route_id=%s, route_short_name=%s, route_long_name=%s, route_type=%s\n", $route_comment, $route_id, $route_short_name, $route_long_name, $route_type  if ( $debug );

        if ( defined $opt_format && $opt_format =~ m/^ptna$/i ) {
            if ( $opt_comment ) {
                if ( $route_comment =~ m/;/ ) {
                    $ptna_comment = '"' . $route_comment . '"';
                } else {
                    $ptna_comment = $route_comment;
                }
            } else {
                $ptna_comment = '';
            }
            if ( $route_short_name =~ m/;/ ) {
                $ptna_ref = '"' . $route_short_name . '"';
            } else {
                $ptna_ref = $route_short_name;
            }
            if ( defined $route_id_2_type{$route_type} ) {
                $ptna_route_type = $route_id_2_type{$route_type};
            } else {
                $ptna_route_type = 'bus';
            }
            if ( $route_long_name ) {
                $ptna_from = $ptna_to = $route_long_name;

                $ptna_from =~ s|\s*[/-].*$||;

                if ( $ptna_from =~ m/;/ ) {
                    $ptna_from = '"' . $ptna_from . '"';
                }
                $ptna_to   =~ s|^.*[/-]\s*||;

                if ( $ptna_to =~ m/;/ ) {
                    $ptna_to = '"' . $ptna_to . '"';
                }
            } else {
                $ptna_from = $ptna_to = '';
            }

            printf STDOUT "%s;%s;%s;%s;%s;%s\n", $ptna_ref, $ptna_route_type, $ptna_comment, $ptna_from, $ptna_to, $ptna_operator;

        } else {
            printf STDOUT "%s\n", $route_short_name;
        }
    }

}


#############################################################################################
#
#
#

sub list_agencies {
    my $sth     = undef;
    my @row     = ();
    my %printed = ();

    $sth = $dbh->prepare( "SELECT agency_name FROM agency" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        printf STDOUT "%s\n", $row[0]   unless ( $printed{$row[0]});

        $printed{$row[0]} = 1;
    }

}


#############################################################################################
#
#
#

sub list_agency_ids {
    my $sth     = undef;
    my @row     = ();
    my %printed = ();

    $sth = $dbh->prepare( "SELECT agency_id FROM agency" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        printf STDOUT "%s\n", $row[0]   unless ( $printed{$row[0]});

        $printed{$row[0]} = 1;
    }

}


#############################################################################################
#
#
#

sub find_agency_id {
    my $agency_name = shift || '*';
    my $sth         = undef;
    my @row         = ();

    $sth = $dbh->prepare( "SELECT agency_id FROM agency WHERE agency_name = '$agency_name'" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        return $row[0];
    }

    return undef;
}


#############################################################################################
#
#
#

sub find_agency_ids {
    my $agency_names    = shift || '*';
    my $sth             = undef;
    my @row             = ();
    my %seen            = ();
    my @return_array    = ();
    my $clause          = '';
    my @bind            = ();

    $clause = "agency_name = '" . join( "' OR agency_name = '", split( ',', $agency_names ) ) . "'";
    printf STDERR "$clause\n";

    @bind   = split( ',', $agency_names );
    $sth = $dbh->prepare( "SELECT agency_id FROM agency WHERE $clause" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        push( @return_array, $row[0] )  unless ( $seen{$row[0]} );
        $seen{$row[0]} = 1;
    }

    return @return_array;
}


#############################################################################################
#
#
#

sub list_route_type {
    my $sth     = undef;
    my @row     = ();
    my %printed = ();

    $sth = $dbh->prepare( "SELECT route_type FROM routes" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array ) {

        printf STDOUT "%s\n", $row[0]   unless ( $printed{$row[0]});

        $printed{$row[0]} = 1;
    }

}


#############################################################################################
#
#
#

sub read_calendar {
    my $sth         = undef;
    my $service_id  = undef;
    my $start_date  = undef;
    my $end_date    = undef;
    my $help        = undef;

    $sth = $dbh->prepare( "SELECT service_id,start_date,end_date FROM calendar" );
    $sth->execute();

    while ( ($service_id,$start_date,$end_date) = $sth->fetchrow_array ) {

        $service_ids{$service_id}->{'start_date'} = $start_date;
        $service_ids{$service_id}->{'end_date'}   = $end_date;
        $help = $start_date . $end_date;
        if ( $help =~ m/^(\d\d\d\d)(\d\d)(\d\d)(\d\d\d\d)(\d\d)(\d\d)$/ ) {
            $service_ids{$service_id}->{'duration'} = Date::Calc::Delta_Days($1,$2,$3,$4,$5,$6);

            #printf STDERR "Service-ID: %s - %s - %s == %s\n", $service_id, $start_date, $end_date, $service_ids{$service_id}->{'duration'};
        }
    }
}


#############################################################################################
#
#
#

sub read_trips {
    my $sth         = undef;
    my $route_id    = undef;
    my $service_id  = undef;

    $sth = $dbh->prepare( "SELECT route_id,service_id FROM trips" );
    $sth->execute();

    while ( ($route_id,$service_id) = $sth->fetchrow_array ) {
        $service_ids_of_route_ids{$route_id}->{$service_id} = 1;
    }
    
    foreach $route_id ( sort ( keys ( %service_ids_of_route_ids ) ) ) {
        printf STDERR "Route-ID: %s - %s\n", $route_id, join( ', ', sort( keys ( %{$service_ids_of_route_ids{$route_id}} ) ) );
    }
}




