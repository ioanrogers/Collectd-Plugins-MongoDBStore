package Collectd::Plugins::MongoDBStore;

# ABSTRACT: write to mongodb
# XXX timeout and reconnect options
# TODO store a document per host containing available plugins, plugin_instances
# and type_instances. Should be okay to keep it in memory...

use v5.10;
use strict;
use warnings;
use Collectd qw/ :all /;
use MongoDB;
use Try::Tiny;

# XXX need it? performance?
#$MongoDB::BSON::looks_like_number = 1;

plugin_register( TYPE_INIT,   'MongoDBStore', 'mdbs_init' );
plugin_register( TYPE_CONFIG, 'MongoDBStore', 'mdbs_config' );
plugin_register( TYPE_WRITE,  'MongoDBStore', 'mdbs_write' );

my $conn;    # connection to Mongodb server

# XXX should these next two be configurable?
my $db;      # our database
my $coll;    # our collection
my $needs_auth = 0;
my $conn_opts  = {};    # MongoDB provides the defaults

sub mdbs_config {
    my $config = shift;

    # TODO multiple hosts?
    my $host;
    my $port;

    foreach my $kv ( @{ $config->{children} } ) {

        my $key   = $kv->{key};
        my $value = $kv->{values}[0];

        given ( $key ) {
            when ( 'Host' ) {
                $host = $value;
            }
            when ( 'Port' ) {
                $port = $value;
            }
            when ( 'NeedsAuth' ) {
                $needs_auth = $value;
            }
            when ( 'Username' ) {
                $conn_opts->{username} = $value;
            }
            when ( 'Password' ) {
                $conn_opts->{password} = $value;
            }
        };
    }

    if ( defined $host ) {
        $conn_opts->{host} = $host;
        if ( defined $port ) {
            $conn_opts->{host} .= ":$port";
        }
    }

    return 1;
}

sub mdbs_init {

    if ( $needs_auth ) {
        if ( !defined $conn_opts->{username} ) {
            plugin_log( LOG_ERR, 'MongoDBStore: No Username configured and NeedsAuth is true.' );
            return 0;
        }
        if ( !defined $conn_opts->{password} ) {
            plugin_log( LOG_ERR, 'MongoDBStore: No Password configured and NeedsAuth is true.' );
            return 0;
        }
    }

    try {
        $conn = MongoDB::Connection->new( $conn_opts );
    }
    catch {
        plugin_log( LOG_ERR, "MongoDBStore: connection error: $_" );
    };

    $db   = $conn->collectd;
    $coll = $db->records;

    # TODO optional capped collection, with configurable size...
    # TODO profile the indexes - timestamp is essential for read performance,
    # not so sure about the others
    $coll->ensure_index( { timestamp => 1 } );

    #$coll->ensure_index({hostname => 1});
    #$coll->ensure_index({plugin => 1});
    #$coll->ensure_index({plugin_instance => 1});
    #$coll->ensure_index({type_instance => 1});

    return 1;
}

sub _get_ds_type_name {
    my $type = shift;
    given ( $type ) {
        when ( 0 ) {
            return 'COUNTER';
        }
        when ( 1 ) {
            return 'GAUGE';
        }
        when ( 2 ) {
            return 'DERIVE';
        }
        when ( 3 ) {
            return 'ABSOLUTE';
        }
        default {
            return 'UNKNOWN';
        }
    };
}

sub mdbs_write {
    my ( $type, $data_set, $value_list ) = @_;

    # TODO utf-8 - any plugins store strings?
    # TODO if there are multiple values, use batch_insert (\@array, $options)
    
    my $i = 0;
    foreach my $ds ( @{$data_set} ) {
        my $doc = {
            timestamp       => $value_list->{'time'},
            host            => $value_list->{host},
            plugin          => $value_list->{plugin},
            plugin_instance => $value_list->{plugin_instance},
            type_instance   => $value_list->{type_instance},
            interval        => $value_list->{interval},
            value           => $value_list->{'values'}[$i],
            type            => $type,
            ds_name         => $ds->{name},
            ds_type         => _get_ds_type_name( $ds->{type} ),
        };

        # XXX any performance issue with safe? Option to disable?
        try {
            my $id = $coll->insert( $doc, { safe => 1 } );
            plugin_log( LOG_DEBUG, "MongoDBStore: inserted new record: $id" );
        } catch {
            plugin_log( LOG_ERR, "MongoDBStore: failed to insert record: $_" );
            return 0;
        };
        $i++;
    }
    return 1;
}

1;

__END__

=head1 SYNOPSIS

 <LoadPlugin perl>
    Globals true
 </LoadPlugin>

 <Plugin perl>
    BaseName "Collectd::Plugins"
    LoadPlugin MongoDBStore
    <Plugin MongoDBStore>
       # All optional
       Host "localhost"
       Port "27017"
       NeedsAuth true
       Username "collectd"
       Password "mysecretpassword"
    </Plugin>
 </Plugin>
