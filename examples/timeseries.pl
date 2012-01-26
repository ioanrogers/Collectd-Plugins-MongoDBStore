#!/usr/bin/env perl

# ABSTRACT: simple timeseries charts to test MongoDBStore

use v5.10;
use strict;
use warnings;
use MongoDB;
use Pod::Usage;
use Try::Tiny;
use Getopt::Long;
use SVG::TT::Graph::TimeSeries;
use HTTP::Date;
use DateTime;
use DateTime::Format::W3CDTF;
use DateTime::Format::Natural;
use Number::Format;

#use Statistics::Descriptive;
use YAML::Any qw/Dump DumpFile/;

my $conn;    # connection to Mongodb server
my $db;      # our database
my $coll;    # our collection
my $opt = {
    width  => 800,
    height => 600,
    from   => '1 hour ago',
    for    => '1',
};

sub create_svg_chart {
    my ( $data, $title ) = @_;
    my $svgtt = SVG::TT::Graph::TimeSeries->new( {
            width            => $opt->{width},
            height           => $opt->{height},
            graph_title      => $title,
            show_graph_title => 1,
            show_data_points => 1,
            rollover_values  => 1, # makes huge files!
            show_data_values => 1,
            #stacked          => 1, # not good if you don't know what the order should be
            #show_y_title      => 1,
            #y_title          => $opt->{plugin}, # don't know what the title is!
            compress        => 0, # pain in the arse for testing
            key               => 1,
            key_position      => 'bottom',
            #tidy => 0, # tidying saves maybe 30% of uncompressed file size, but takes 10x longer! 
            # it also doesn't compress any better than untidied
            #y_label_formatter => sub {
            #     # XXX be nice if plugins would tell us their units
            #    return Number::Format::format_bytes( $_[0] );
            #},
            timescale_divisions => '10 minutes',
            x_label_format    => '%H:%M',
            # data_value_format => '&lt;%.2f&gt;'
            style_sheet => 'svg.css',
            min_timescale_value => $opt->{from_dt}->iso8601,
            max_timescale_value => $opt->{to_dt}->iso8601,
            
    } );
    
    if ( ref $data eq 'ARRAY' ) {

        # only one data set, and no type_instance
        $svgtt->key(0);
        $svgtt->add_data( {
                data  => $data,
        } );
    } else {
        foreach my $inst ( keys %{$data} ) {
            $svgtt->add_data( {
                    data  => $data->{$inst},
                    title => $inst,
            } );
        }
    }

    my $svg = $svgtt->burn;
    open my $fh, '>', "$title.svg"
      or die;

    $fh->print( $svg );
    $fh->close;

}

sub get_chart_title {

    my $title = sprintf '%s-%s', $opt->{host}, $opt->{plugin};
    if ( defined $opt->{plugin_instance} ) {
        $title .= "-" . $opt->{plugin_instance};
    }

    return $title;
}

sub connect_to_mongo {
    my $conn_opts = {};

    if ( defined $opt->{mongohost} ) {
        $conn_opts->{host} = $opt->{mongohost};
    }

    if ( defined $opt->{mongouser} ) {
        $conn_opts->{username} = $opt->{mongouser};

        if ( !defined $opt->{mongopass} ) {
            die 'mongouser set, but no mongopass';
        }
    }

    $conn = MongoDB::Connection->new( $conn_opts );

    $db   = $conn->collectd;
    $coll = $db->records;

    return 1;
}

sub get_data {

    my $data;
    my $query = {
        host      => $opt->{host},
        plugin    => $opt->{plugin},
        timestamp => {
            '$gte' => $opt->{from_dt}->epoch,
            '$lte' => $opt->{to_dt}->epoch,
        },
    };

    if ( defined $opt->{plugin_instance} ) {
        $query->{plugin_instance} = $opt->{plugin_instance};
    }

    if ( defined $opt->{type_instance} ) {
        if ( scalar @{ $opt->{type_instance} } == 1 ) {

            # only one type
            $query->{type_instance} = $opt->{type_instance}->[0];
        } else {
            $query->{type_instance} = { '$in' => $opt->{type_instance} };
        }

    }

    my $cursor = $coll->find( $query )->fields( {
            timestamp       => 1,
            value           => 1,
            type_instance   => 1,
            plugin_instance => 1,
        } )->sort( { timestamp => 1, } );

    my $dtf = DateTime::Format::W3CDTF->new;
    while ( my $object = $cursor->next ) {
        
        # convert date format for SVG:TT
        my $dt       = DateTime->from_epoch( epoch => $object->{timestamp} );
        my $date_str = $dtf->format_datetime( $dt );
        my $record   = [ $date_str, $object->{value} ];
        
        if ( defined $object->{type_instance} ) {
            push @{ $data->{ $object->{type_instance} } }, $record;
        } else {
            push @{$data}, $record;
        }
    }
    
    if (!defined $data) {
         #no records found, but SVG::TT needs array
         $data = [];
    }
    
    return $data;
}

sub get_list {
    my ( $key, $query ) = @_;

    my $result = $db->run_command( [
            distinct => 'records',
            key      => $key,
            query    => $query,
    ] );

    return $result;
}

sub show_host_list {

    my $result = get_list( 'host' );

    say "\nAvailable hosts";
    foreach ( sort @{ $result->{values} } ) {
        say;
    }

}

sub show_plugin_list {

    if ( !defined $opt->{host} ) {
        pod2usage( 'You must specify a host to list plugins' );
    }
    my $result = get_list( 'plugin', { host => $opt->{host} } );

    say "\nAvailable plugins for " . $opt->{host};
    foreach ( sort @{ $result->{values} } ) {
        say;
    }

}

sub show_instance_list {
    my $type = shift;

    if ( !defined $opt->{host} ) {
        pod2usage( -verbose => 1, -message => 'You must specify a host to list plugin instances' );
    }

    if ( !defined $opt->{plugin} ) {
        pod2usage( -verbose => 1, -message => 'You must specify a plugin to list instances for' );
    }

    my $key = 'plugin_instance';
    if ( defined $type ) {
        $key = 'type_instance';
    }

    my $result = get_list( $key, { host => $opt->{host}, plugin => $opt->{plugin} } );

    printf "\nAvailable %ss for %s/%s\n", $key, $opt->{host}, $opt->{plugin};

    if ( scalar @{ $result->{values} } == 1 ) {
        say "NONE";
        return;
    }

    foreach ( sort @{ $result->{values} } ) {
        say;
    }

}

my $getopt = GetOptions(
    $opt,               'help|h!', 'mongohost=s', 'mongouser=s',
    'mongopass=s',      'host=s',  'plugin=s',    'plugin_instance=s',
    'type_instance=s@', 'list=s',  'width|w=i',   'height|h=i',
    'from=s',           'for=s'
);

if ( defined $opt->{help} ) {
    pod2usage( 1 );
}

connect_to_mongo;

# check type_instances
if ( defined $opt->{type_instance} ) {
    @{ $opt->{type_instance} } = split( /,/, join( ',', @{ $opt->{type_instance} } ) );
}

if ( defined $opt->{list} ) {
    given ( $opt->{list} ) {
        when ( /^host$/i ) {
            show_host_list;
        }
        when ( /^plugin$/i ) {
            show_plugin_list;
        }
        when ( /^type_inst/i ) {
            show_instance_list( 1 );
        }
        when ( /^plugin_inst/i ) {
            show_instance_list;
        }
        default {
            pod2usage( -verbose => 1, -message => "Unknown 'list' value: " . $opt->{list} );
        }
    };
    exit;
}

if ( !defined $opt->{host} ) {
    pod2usage( 'Which host do you want to generate a chart for?' );
}

if ( !defined $opt->{plugin} ) {
    pod2usage( 'Which plugin do you want to generate a chart for?' );
}

# find out start time
my $dtfn = DateTime::Format::Natural->new( time_zone => "local" );
$opt->{from_dt} = $dtfn->parse_datetime( $opt->{from} );
$opt->{to_dt} = $opt->{from_dt}->clone->add( hours => $opt->{for} );

my $data  = get_data;
my $title = get_chart_title;

create_svg_chart( $data, $title );

__END__

=head1 SYNOPSIS
 
timeseries.pl [options]
   
=head1 OPTIONS
 
=over

=item B<--help|h>
 
Print a brief help message and exits.
 
=item B<--host> <hostname>
 
Generate a chart for this host. Name is as collectd stores it, so check your settings.

See B<list>.

=item B<--plugin> <plugin_name>
 
The collectd plugin to get data from.

Requires B<--host>.

See B<list>.

=item B<--plugin_instance> <plugin_instance>
 
The instance of the specified plugin. e.g the B<cpu> plugin has one plugin_instance
per CPU core.

Requires B<--plugin>.

See B<list>.

=item B<--type_instance> <type_instance>
 
The plugin's type instance. e.g. the B<cpu> plugin has type_instances for
user, system, nice, etc.

By default, all available type_instances will be used.

See B<list>.

=item B<--list> <list_type>
 
Checks with MongoDB for available:

=over

=item * host

available hostnames

=item * plugin

available plugins for a particular host
( requires B<--host> )

=item * plugin_instance

available plugin instances for a particular host and plugin
( requires B<--host> and B<--plugin> )

=item * type_instance

available type_instances for a particular host and plugin
( requires B<--host> and B<--plugin> )

=back

=item B<--mongohost> <mongodb_host>

Same options as MongoDB host option. Defaults to C<localhost:27017>

=item B<--mongouser> <username>

=item B<--mongopass> <password>

If your mongod required authentication, specify the username and password here.

=item B<--width> <integer>
=item B<--height> <integer>

Width and height of the chart. Defaults to 800x600

=item B<--from> <start_datetime>

Time chart starts at, in a format recognisable to L<DateTime::Format::Natural>.
e.g. '4 hours ago', 'yesterday'

Defaults to '1 hour ago'.

=item B<--for> <integer>

Duration of time to graph in hours. Defaults to 1 hour.

=back
