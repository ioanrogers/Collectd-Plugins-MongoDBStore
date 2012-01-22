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
use YAML::Any;
use HTTP::Date;
use Chart::Clicker;
use Chart::Clicker::Data::Series;
use Chart::Clicker::Data::DataSet;
use Chart::Clicker::Renderer::Line;
use Chart::Clicker::Axis::DateTime;

my $conn;    # connection to Mongodb server
my $db;      # our database
my $coll;    # our collection
my $opt = {};

sub create_clicker_chart {

    my ( $data, $title ) = @_;

    my $cc = Chart::Clicker->new( width => 1024, height => 768 );
    $cc->title->text($title);

    my %cc_values;
    foreach my $d ( @{$data} ) {
        my $k = str2time $d->[0];
        $cc_values{$k} = $d->[1];
    }
    say Dump( \%cc_values );
    my $cc_series = Chart::Clicker::Data::Series->new( \%cc_values );

    my $cc_dataset = Chart::Clicker::Data::DataSet->new( series => [$cc_series] );
    $cc->add_to_datasets($cc_dataset);

    my $ctx = $cc->get_context('default');

    my $dtaxis = Chart::Clicker::Axis::DateTime->new(
        format      => '%H:%M:%S',
        position    => 'bottom',
        orientation => 'horizontal'
    );
    $ctx->domain_axis($dtaxis);

    my $cc_renderer = Chart::Clicker::Renderer::Line->new;
    $cc->set_renderer($cc_renderer);
    $cc->write_output("$title.png");

}

sub create_svg_chart {
    my ( $data, $title ) = @_;
    my $svgtt = SVG::TT::Graph::TimeSeries->new(
        {
            width            => 1024,
            height           => 768,
            graph_title      => $title,
            show_graph_title => 1,
        }
    );

    $svgtt->add_data(
        {
            data  => $data,
            title => $opt->{plugin},
        }
    );

    my $svg = $svgtt->burn;
    open my $fh, '>', "$title.svg"
      or die;

    $fh->print($svg);
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

    $conn = MongoDB::Connection->new($conn_opts);

    $db   = $conn->collectd;
    $coll = $db->records;

    return 1;
}

sub get_data {

    my @data;
    my $query = {
        host   => $opt->{host},
        plugin => $opt->{plugin},
    };

    # XXX check there's only one plugin instance?
    if ( defined $opt->{plugin_instance} ) {
        $query->{plugin_instance} = $opt->{plugin_instance};
    }

    if ( defined $opt->{type_instance} ) {
        $query->{type_instance} = $opt->{type_instance};
    }

    my $cursor = $coll->find($query)->fields(
        {
            timestamp => 1,
            value     => 1,
        }
    )->sort( { timestamp => 1, } );

    while ( my $object = $cursor->next ) {

        #        printf "%s %s %s\n", $object->{timestamp}, $object->{value};
        push @data, [ time2str( $object->{timestamp} ), $object->{value} ];
    }

    return \@data;
}

sub show_host_list {
    my $result = $db->run_command(
        [
            distinct => 'records',
            key      => 'host',
        ]
    );

    say "\nAvailable hosts";
    foreach ( @{ $result->{values} } ) {
        say;
    }

}

sub show_plugin_list {

    if ( !defined $opt->{host} ) {
        pod2usage('You must specify a host to list plugins');
    }

    my $result = $db->run_command(
        [
            distinct => 'records',
            key      => 'plugin',
            query    => { host => $opt->{host} },
        ]
    );

    say "\nAvailable plugins for " . $opt->{host};
    foreach ( @{ $result->{values} } ) {
        say;
    }

}

sub show_instance_list {
    my $type = shift;

    if ( !defined $opt->{host} ) {
        pod2usage('You must specify a host to list plugin instances');
    }

    if ( !defined $opt->{plugin} ) {
        pod2usage('You must specify a plugin to list instances for');
    }

    my $key = 'plugin_instance';
    if ( defined $type ) {
        $key = 'type_instance';
    }
    my $result = $db->run_command(
        [
            distinct => 'records',
            key      => $key,
            query    => { host => $opt->{host}, plugin => $opt->{plugin} },
        ]
    );

    printf "\nAvailable %ss for %s/%s\n", $key, $opt->{host}, $opt->{plugin};

    if ( scalar @{ $result->{values} } == 1 ) {
        say "NONE";
        return;
    }

    foreach ( @{ $result->{values} } ) {
        say;
    }

}

my $getopt = GetOptions(
    $opt,       'help|h!',           'mongohost=s', 'plugin=s',
    'debug|D!', 'mongouser=s',       'mongopass=s', 'list=s',
    'host=s',   'plugin_instance=s', 'type_instance=s'
);

if ( defined $opt->{help} ) {
    pod2usage;
}

connect_to_mongo;

if ( defined $opt->{list} ) {
    given ( $opt->{list} ) {
        when (/^host$/i) {
            show_host_list;
        }
        when (/^plugin$/i) {
            show_plugin_list;
        }
        when (/^type_inst/i) {
            show_instance_list(1);
        }
        when (/^plugin_inst/i) {
            show_instance_list;
        }
        default {
            pod2usage( "Unknown 'list' value: " . $opt->{list} );
        }
    };
    exit;
}

if ( !defined $opt->{host} ) {
    pod2usage('Which host do you want to generate a chart for?');
}

if ( !defined $opt->{plugin} ) {
    pod2usage('Which plugin do you want to generate a chart for?');
}

my $data = get_data;

my $title = get_chart_title;
create_clicker_chart( $data, $title );
create_svg_chart( $data, $title );

__END__

=head1 SYNOPSIS
 
timeseries.pl --host <hotname> [options]
 
 Options:
   --help            brief help message
   --host            host to view, or 'list'
   
=head1 OPTIONS
 
=over 8
 
=item <b>--help</b>
 
Print a brief help message and exits.
 
=item <b>--host</b>
 
The host to generate a chart for. Use 'list' to show available hosts.

=item <b>--host</b>
 
The host to generate a chart for

=back

