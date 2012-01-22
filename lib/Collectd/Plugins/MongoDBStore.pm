package Collectd::Plugins::MongoDBStore;

# ABSTRACT: write to mongodb
# XXX timeout and reconnect options

use v5.10;
use strict;
use warnings;
use Collectd qw/ :all /;
use MongoDB;
use Try::Tiny;

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

        given ($key) {
            when ('Host') {
                $host = $value;
            }
            when ('Port') {
                $port = $value;
            }
            when ('NeedsAuth') {
                $needs_auth = $value;
            }
            when ('Username') {
                $conn_opts->{username} = $value;
            }
            when ('Password') {
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

    if ($needs_auth) {
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
        $conn = MongoDB::Connection->new($conn_opts);
    } catch {
        plugin_log( LOG_ERR, "MongoDBStore: connection error: $_" );
    };
            
    $db   = $conn->collectd;
    $coll = $db->records;

    return 1;
}

sub mdbs_write {
    my ( $type, $data_set, $value_list ) = @_;

    # TODO if there are multiple values, use batch_insert (\@array, $options)
    foreach my $ds ( @{$data_set} ) {
        my $i = 0;

        my $doc = {
            timestamp       => $value_list->{'time'},
            value           => $value_list->{'values'}[$i],
            host            => $value_list->{host},
            plugin          => $value_list->{plugin},
            plugin_instance => $value_list->{plugin_instance},
            type            => $type,
            type_instance   => $value_list->{type_instance},
            ds_type         => $ds->{type},                      # XXX use an enum
        };

        try {
            my $id = $coll->insert( $doc, { safe => 1 } );
            plugin_log( LOG_DEBUG, "MongoDBStore: inserted new record: $id" );
        }
        catch {
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

