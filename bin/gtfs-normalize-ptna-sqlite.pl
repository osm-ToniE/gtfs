#!/usr/bin/perl

use warnings;
use strict;

####################################################################################################################
#
#
#

use POSIX;

use utf8;
binmode STDIN,  ":encoding(UTF-8)";
binmode STDOUT, ":encoding(UTF-8)";
binmode STDERR, ":encoding(UTF-8)";
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
my $agency                   = undef;
my $consider_calendar        = undef;
my $language                 = 'de';

GetOptions( 'debug'                 =>  \$debug,                 # --debug
            'verbose'               =>  \$verbose,               # --verbose
            'agency=s'              =>  \$agency,                # --agency=
            'consider-calendar'     =>  \$consider_calendar,     # --consider-calendar
            'language=s'            =>  \$language,              # --language=de
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

my $dbh = DBI->connect( "DBI:SQLite:dbname=$DB_NAME", "", "", { AutoCommit => 0, RaiseError => 1 } ) or die $DBI::errstr;


####################################################################################################################
#
#
#

my ($sec,$min,$hour,$day,$month,$year) = localtime();

my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );

my $start_time = time();

CreatePtnaNormalization();

ClearNormalizationRouteLongName();

my $normalized_routes = NormalizeRouteLongName();

ClearNormalizationStopName();

my $normalized_stops  = NormalizeStopName();

UpdatePtnaNormalization( time() - $start_time, $normalized_routes, $normalized_stops );

exit 0;


#############################################################################################
#
#
#

sub CreatePtnaNormalization {

    my $sth = undef;

    my ($sec,$min,$hour,$day,$month,$year) = localtime();

    my $today = sprintf( "%04d-%02d-%02d", $year+1900, $month+1, $day );

    $sth  = $dbh->prepare( "DROP TABLE IF EXISTS ptna_normalization;" );
    $sth->execute();

    $sth = $dbh->prepare( "CREATE TABLE ptna_normalization (
                                        'id'                    INTEGER DEFAULT 0 PRIMARY KEY,
                                        'date'                  TEXT,
                                        'duration'              INTEGER DEFAULT 0,
                                        'routes'                INTEGER DEFAULT 0,
                                        'stops'                 INTEGER DEFAULT 0 );" );
    $sth->execute();

    $sth = $dbh->prepare( "INSERT INTO ptna_normalization
                             (id,date)
                      VALUES (1, ?);" );
    $sth->execute( $today );

    $sth = $dbh->prepare( "UPDATE ptna SET normalized=? WHERE id=1;" );
    $sth->execute( $today );

    $dbh->commit();

    return;
}


#############################################################################################
#
#
#

sub UpdatePtnaNormalization {
    my $seconds = shift || 1;
    my $routes  = shift || 0;
    my $stops   = shift || 0;

    my $sth     = $dbh->prepare( "UPDATE ptna_normalization SET date=?,duration=?,routes=?,stops=? WHERE id=1;" );

    $sth->execute( $today, $seconds, $routes, $stops );

    $dbh->commit();

    return 0;
}


#############################################################################################
#
#
#

sub ClearNormalizationRouteLongName {

    my $sth                     = undef;
    my $has_normalized_column   = undef;
    my @row                     = ();

    $sth = $dbh->prepare( "PRAGMA table_info(ptna_routes);" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[1] && $row[1] eq 'normalized_route_long_name' ) {
            $has_normalized_column = 1;
            last;
        }
    }

    if ( $has_normalized_column ) {
        $sth  = $dbh->prepare( "UPDATE ptna_routes SET normalized_route_long_name='' WHERE normalized_route_long_name != '';" );
        $sth->execute();
    }

    $dbh->commit();

    return;
}


#############################################################################################
#
#
#

sub NormalizeRouteLongName {

    my $sth                     = undef;
    my $sth2                    = undef;
    my @row                     = ();
    my $original                = '';
    my $normalized              = '';
    my $route_id                = '';
    my $number_of_routes        = 0;
    my $number_of_normalized    = 0;
    my $has_normalized_column   = 0;

    printf STDERR "Routes normalized: %06d of %06d\r", $number_of_normalized, $number_of_routes     if ( $verbose );

    $sth = $dbh->prepare( "PRAGMA table_info(ptna_routes);" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[1] && $row[1] eq 'normalized_route_long_name' ) {
            $has_normalized_column = 1;
            last;
        }
    }

    if ( !$has_normalized_column ) {
        $sth = $dbh->prepare( "ALTER TABLE ptna_routes ADD normalized_route_long_name TEXT DEFAULT '';" );
        $sth->execute();
    }

    $sth = $dbh->prepare( "SELECT COUNT(*) FROM routes;" );
    $sth->execute();

    if ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            $number_of_routes = $row[0];
        }
    }

    printf STDERR "Routes normalized: %06d of %06d\r", $number_of_normalized, $number_of_routes     if ( $verbose );

    $sth = $dbh->prepare( "SELECT route_long_name,route_id FROM routes;" );
    $sth->execute();

    $sth2 = $dbh->prepare( "REPLACE INTO ptna_routes (normalized_route_long_name,route_id) VALUES (?,?);" );

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0] && $row[1]  ) {
            $original = decode( 'utf8',  $row[0] );
            $route_id = $row[1];

            $normalized = NormalizeString( $original );

            if ( $normalized ne $original ) {
                $sth2->execute( $normalized, $route_id );

                $number_of_normalized++;

                #printf STDERR "Route: %s -> %s\n", $original, $normalized;
                printf STDERR "Routes normalized: %06d of %06d\r", $number_of_normalized, $number_of_routes     if ( $verbose );
            }
        }
    }

    printf STDERR "Routes normalized: %06d of %06d\n", $number_of_normalized, $number_of_routes     if ( $verbose );

    $dbh->commit();

    return $number_of_normalized;
}


#############################################################################################
#
#
#

sub ClearNormalizationStopName {

    my $sth                     = undef;
    my $has_normalized_column   = undef;
    my @row                     = ();


    $sth = $dbh->prepare( "PRAGMA table_info(ptna_stops);" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[1] && $row[1] eq 'normalized_stop_name' ) {
            $has_normalized_column = 1;
            last;
        }
    }

    if ( $has_normalized_column ) {
        $sth = $dbh->prepare( "UPDATE ptna_stops SET normalized_stop_name='' WHERE normalized_stop_name != '';" );
        $sth->execute();
    }

    $dbh->commit();

    return;
}


#############################################################################################
#
#
#

sub NormalizeStopName {

    my $sth                     = undef;
    my $sth2                    = undef;
    my @row                     = ();
    my $original                = '';
    my $normalized              = '';
    my $stop_id                 = '';
    my $number_of_stops         = 0;
    my $number_of_normalized    = 0;
    my $has_normalized_column   = 0;

    printf STDERR "Stops  normalized: %06d of %06d\r", $number_of_normalized, $number_of_stops     if ( $verbose );

    $sth = $dbh->prepare( "PRAGMA table_info(ptna_stops);" );
    $sth->execute();

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[1] && $row[1] eq 'normalized_stop_name' ) {
            $has_normalized_column = 1;
            last;
        }
    }

    if ( !$has_normalized_column ) {
        $sth = $dbh->prepare( "ALTER TABLE ptna_stops ADD normalized_stop_name TEXT DEFAULT '';" );
        $sth->execute();
    }

    $sth = $dbh->prepare( "SELECT COUNT(*) FROM stops;" );
    $sth->execute();

    if ( @row = $sth->fetchrow_array() ) {
        if ( $row[0]  ) {
            $number_of_stops = $row[0];
        }
    }

    printf STDERR "Stops  normalized: %06d of %06d\r", $number_of_normalized, $number_of_stops     if ( $verbose );

    $sth = $dbh->prepare( "SELECT stop_name,stop_id FROM stops;" );
    $sth->execute();

    $sth2 = $dbh->prepare( "REPLACE INTO ptna_stops (normalized_stop_name,stop_id) VALUES (?,?);" );

    while ( @row = $sth->fetchrow_array() ) {
        if ( $row[0] && $row[1]  ) {
            $original = decode( 'utf8',  $row[0] );
            $stop_id  = $row[1];

            $normalized = NormalizeString( $original );

            if ( $normalized ne $original ) {
                $sth2->execute( $normalized, $stop_id );

                $number_of_normalized++;

                printf STDERR "Stop %s: %s -> %s\n", $stop_id, $original, $normalized                          if ( $verbose );
                printf STDERR "Stops  normalized: %06d of %06d\r", $number_of_normalized, $number_of_stops     if ( $verbose );
            }
        }
    }

    printf STDERR "Stops  normalized: %06d of %06d\n", $number_of_normalized, $number_of_stops     if ( $verbose );

    $dbh->commit();

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
        if ( $language =~ m/^de/ ) {
            $normalized =~ s/,/, /g;
            if ( $language eq 'de_CH' ) {
                $normalized =~ s|Str\/|Strasse/|g;
                $normalized =~ s|str\./|strasse/|g;
                $normalized =~ s/Str\./Strasse/g;
                $normalized =~ s/str\./strasse/g;
                $normalized =~ s/Str,/Strasse,/g;
                $normalized =~ s/str,/strasse,/g;
                $normalized =~ s/Str$/Strasse/g;
                $normalized =~ s/str$/strasse/g;
            } else {
                $normalized =~ s|Str\/|Straße/|g;
                $normalized =~ s|str\./|straße/|g;
                $normalized =~ s/Str\./Straße/g;
                $normalized =~ s/str\./straße/g;
                $normalized =~ s/Str,/Straße,/g;
                $normalized =~ s/str,/straße,/g;
                $normalized =~ s/Str$/Straße/g;
                $normalized =~ s/str$/straße/g;
            }
            if ( $language eq 'de_AT' ) {
                $normalized =~ s/St\.-/Sankt /g;
                $normalized =~ s/St\./Sankt /g;
                $normalized =~ s/Abzw /Abzweigung /g;
            }
            $normalized =~ s/\(b\./(bei /g;
            $normalized =~ s/ b\./ bei /g;
            $normalized =~ s/nchnerStr/nchner Str/g;
            $normalized =~ s|Pl\./|Platz/|g;
            $normalized =~ s|pl\./|platz/|g;
            $normalized =~ s|Pl\.\)|Platz)|g;
            $normalized =~ s|pl\.\)|platz)|g;
            $normalized =~ s/Pl\./Platz /g;
            $normalized =~ s/pl\./platz /g;
            $normalized =~ s/Bf\./Bahnhof/g;
            $normalized =~ s/Abzw\./Abzweig /g;
            $normalized =~ s/Abzw /Abzweig /g;
            $normalized =~ s/FFB/Fürstenfeldbruck/g;
            $normalized =~ s/Lkr\.\s*FS/Lkr.Freising/g;
            $normalized =~ s/rstenfeldbr,/rstenfeldbruck, /g;
            $normalized =~ s/rstenfeldbr\.,/rstenfeldbruck, /g;
            $normalized =~ s/rstenfeldb\.,/rstenfeldbruck, /g;
            $normalized =~ s/Gym\./Gymnasium /g;
            $normalized =~ s/Gymn\./Gymnasium /g;
            $normalized =~ s/Hochschule M\. \(Lothstraße\)/Hochschule München (Lothstraße)/g;
            $normalized =~ s/Unterschlei.h\./Unterschleißheim/g;
            $normalized =~ s/Garch\.,\s*Forschungsz\./Garching, Forschungszentrum/g;
            $normalized =~ s/A\.-Stifter/Adalbert-Stifter/g;
            $normalized =~ s|Landsh\./Tuchinger Str|Landshuter/Tuchinger Str|g;
            $normalized =~ s|Arabellap\./Kl\.Bogenh\.|Arabellapark/Klinik Bogenhausen|g;
            $normalized =~ s/Hans-Stie.b\.-Stra.e\s*\(Schleife\)/Hans-Stießberger-Straße (Schleife)/g;
            $normalized =~ s/H\.c-Stie.b\.-Stra.e\s*\(Schleife\)/Hans-Stießberger-Straße (Schleife)/g;
            $normalized =~ s/H\.-Stie.b\.-Stra.e\s*\(Schl\.\)/Hans-Stießberger-Straße (Schleife)/g;
            $normalized =~ s/H\.-Stie.b\.-Str\.\s*\(Schl\.\)/Hans-Stießberger-Straße (Schleife)/g;
            $normalized =~ s/H\.Stie.b\.-Str\.(Schleife)/Hans-Stießberger-Straße (Schleife)/g;
            $normalized =~ s/Aschheim,\s*Siedl\.Tassilo/Aschheim, Siedlung Tassilo/g;
            $normalized =~ s/Max-Planck-Inst\./Max-Planck-Institut/g;
            $normalized =~ s/Oberschl\./Oberschleißheim/g;
            $normalized =~ s/Oberpf\./Oberpaffenhofen/g;
            $normalized =~ s/Gilch\./Gilching/g;
            $normalized =~ s/I\.-Taschner.Gymn/Ignaz-Taschner Gymn/g;
            $normalized =~ s/Fischer-v\.-Erlach-Stra.e/Fischer-von-Erlach-Straße/g;
            $normalized =~ s|F\.-Kobell-Straße/Waldfr\.|Ferdinand-Kobell-Straße/Waldfriedhof|g;
            $normalized =~ s/A\.-Kasperbauer-Str/Andreas-Kasperbauer-Str/g;
            $normalized =~ s/Brunnthal,\s*E\.-Sänger-Ring/Brunnthal, Eugen-Sänger-Ring/g;
            $normalized =~ s/Max-v\.-Eyth-Str/Max-von-Eyth-Str/g;
            $normalized =~ s/C-v\.-Linde-Str/Carl-von-Linde-Str/g;
            $normalized =~ s/R\.-Bosch-Str/Robert-Bosch-Str/g;
            $normalized =~ s/St-Margar\.-Str/Sankt-Margarethen-Str/g;
            $normalized =~ s/Mittenh\. Str/Mittenheimer Str/g;
            $normalized =~ s/M\.-Haslbeck/Michael-Haslbeck/g;
            $normalized =~ s/W\.-Heisenberg-W\./Werner-Heisenberg-Weg/g;
            $normalized =~ s/K\.-Rupprecht-Str/Kronprinz-Rupprecht-Str/g;
            $normalized =~ s/Wernh\.-v\.-Braun/Wernher-von-Braun/g;
            $normalized =~ s/Wernh\.-v-Braun/Wernher-von-Braun/g;
            $normalized =~ s/M\.\s*Schwaben,\s*Wittelsb\.Weg/Markt Schwaben, Wittelsbacher Weg/g;
            $normalized =~ s/Korb\.-Aigner/Korbinian-Aigner/g;
            $normalized =~ s/Unter\.\s*Markt/Unterer Markt/g;
            $normalized =~ s/Markt Indersdorf,\s*Rothbachbr\./Markt Indersdorf, Rothbachbrücke/g;
            $normalized =~ s/Wiedenzh\./Wiedenzhausen/g;
            $normalized =~ s/Gew\.park Römerw\./Gewerbepark Römerweg/g;
            $normalized =~ s/Steink\./Steinkirchen/g;
            $normalized =~ s/W.rmk\./Würmkanal/g;
            $normalized =~ s/tra.e(\d)/traße \1/g;
            $normalized =~ s/Kolumbuspl\./Kolumbusplatz /g;
            $normalized =~ s/Feringastra.eOst/Feringastraße Ost/g;
            $normalized =~ s/Ludw\.-Ganghofer/Ludwig-Ganghofer/g;
            $normalized =~ s/Lindenbg\.Siedlg\./Lindenberg Siedlung/g;
            $normalized =~ s/Parkpl\./Parkplatz/g;
            $normalized =~ s/Haus d\.Kunst/Haus der Kunst/g;
            $normalized =~ s/Lkr\.\s*/Lkr. /g;
            $normalized =~ s/Gewerbegeb\./Gewerbegebiet/g;
            $normalized =~ s/R\.-Diesel/Rudolf-Diesel/g;
            $normalized =~ s/J\.-u\.-R\.-Werner-Platz/Josef-und-Rosina-Werner-Platz/g;
            $normalized =~ s/Buchenauer S\./Buchenauer Straße/g;
            $normalized =~ s/Th\.-Heuss/Theodor-Heuss/g;
            $normalized =~ s/K\.-Adenauer/Konrad-Adenauer/g;
            $normalized =~ s/H\.-Tassilo-Realschule/Herzog-Tassilo-Realschule/g;
            $normalized =~ s/Kerschenst\.Schule/Kerschensteiner Schule/g;
            $normalized =~ s/Kerschenst\./Kerschensteiner/;
            $normalized =~ s/Pestalozzisch\./Pestalozzischule/g;
            $normalized =~ s/Wittelsbach\. Schule/Wittelsbacher Schule/g;
            $normalized =~ s/Freising,\s*RS Gute .nger/Freising, Realschule Gute Änger/g;
            $normalized =~ s/J\.-Dosch-Schule/Josef-Dosch-Schule/g;
            $normalized =~ s/Gudrunsiedlg\./Gudrunsiedlung/g;
            $normalized =~ s/H\.-Hildebrand-Weg/Heinrich-Hildebrand-Weg/g;
            $normalized =~ s/Oskar-v\.-Miller-Str/Oskar-von-Miller-Str/g;
            $normalized =~ s/Taufki\.Stra/Taufkirchener Stra/g;
            $normalized =~ s/H.henkirchen-S\./Höhenkirchen-Siegertsbrunn/g;
            $normalized =~ s/Sch.ng\., Rothschwaiger Str/Schöngeising, Rothschwaiger Str/g;
            $normalized =~ s/Puchh\. Bahnhof/Puchheim Bahnhof/g;
            $normalized =~ s/Puchheim Bf,/Puchheim Bahnhof,/g;
            $normalized =~ s/Ob\. Lagerstr/Obere Lagerstr/g;
            $normalized =~ s|Ottobr\.,\s*A\.Brunneck/Uhlands\.|Ottobrunn, Am Brunneck/Uhlandstraße|g;
            $normalized =~ s/Pfr-Caspar-Mayr-Pl/Pfarrer-Caspar-Mayr-Pl/g;
            $normalized =~ s/Pfr-Caspar-Mayr-Pl/Pfarrer-Caspar-Mayr-Pl/g;
            $normalized =~ s/Friedh\. Schopflach/Friedhof Schopflach/g;
            $normalized =~ s/Tegerns\. Landstr/Tegernseer Landstr/g;
            $normalized =~ s/C\.-Duisberg-Str/Carl-Duisberg-Str/g;
            $normalized =~ s/Andr\.-Wagner-Str/Andreas-Wagner-Str/g;
            $normalized =~ s/F\.-Schüle-Str/Friedrich-Schüle-Str/g;
            $normalized =~ s/H\.-Junkers-Str/Hugo-Junkers-Str/g;
            $normalized =~ s/Bernh\.-Rößner-Str/Bernhard-Rößner-Str/g;
            $normalized =~ s/W\.-Messerschmitt-Str/Werner-Messerschmitt-Str/g;
            $normalized =~ s/Am M.hlstetter Gr\./Am Mühlstetter Graben/g;
            $normalized =~ s/Am M.hlstetter Gr$/Am Mühlstetter Graben/g;
            $normalized =~ s/Brunnth\.Stra/Brunnthaler Stra/g;
            $normalized =~ s/Hallberg\./Hallbergmoos/g;
            $normalized =~ s/Hörgertshs\./Hörgertshausen/g;
            $normalized =~ s/Kottg\. Villenstr/Kottgeisering. Villenstr/g;
            $normalized =~ s|M.hlfeldstr./Freibad|Brunnthaler Stra|g;
            $normalized =~ s/O\.-Kubel-Str/Otto-Kubel-Str/g;
            $normalized =~ s/Isar-Amper-Kl\./Isar-Amper-Klinik/g;
            $normalized =~ s/Gelting \(bei WOR\)/Gelting (bei Wolfratshausen)/g;
            $normalized =~ s/Pfaffenr\.Stra/Pfaffenrieder Stra/;
            $normalized =~ s/Dr\.-H\.-Eisenmann-Str/Dr.-Hans-Eisenmann-Str/;
            $normalized =~ s/Pf\.-Freiberger-Str/Pfarrer-Freiberger-Str/;
            $normalized =~ s/Südl\. Ingolstädter Str/Südliche Ingolstädter Str/;
            $normalized =~ s/Oskar-M\.-Graf-Str/Oskar-Maria-Graf-Str/;
            $normalized =~ s/O-M-Graf-Str/Oskar-Maria-Graf-Str/;
            $normalized =~ s/A\.-J\.-Lippl-Str/Alois-Johannes-Lippl-Str/;
            $normalized =~ s/P\.-Rupert-Mayer-Str/Pater-Rupert-Mayer-Str/;
            $normalized =~ s/R\.-Strauss-Str/Richard-Strauss-Str/;
            $normalized =~ s/Bernhard-Rö.ner-St$/Bernhard-Rößner-Straße/;
            $normalized =~ s/Haslach \(Landkreis EBE\)/Haslach (Landkreis Ebersberg)/;
            $normalized =~ s/Pf\.-Aigner-Allee/Pfarrer-Aigner-Allee/;
            $normalized =~ s/Hl\. Blut/Heilig Blut/;
            $normalized =~ s/Hallbergm\./Hallbergmoos/;
            $normalized =~ s/Bildungsz\./Bildungszentrum/;
            $normalized =~ s/\s+/ /g;
            $normalized =~ s/^\s//g;
            $normalized =~ s/\s$//g;
            $normalized =~ s/\s,/,/g;
            $normalized =~ s/\( /(/g;
            $normalized =~ s/ \)/)/g;
#            $normalized =~ s|/ |/|g;
#            $normalized =~ s| /|/|g;
        }
    }

    return $normalized;

}
