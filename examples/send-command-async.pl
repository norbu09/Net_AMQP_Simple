#!/usr/bin/perl

# this client sends a command off to a bot and tells the bot (in the
# JSON structure) where to send the reply to. this is _not_ a AMQP reply
# header but a protocol on top of AMQP. This is only a demo of how i use
# this lib this is _not_ the only way of doing things and i understrand
# that AMQP reply headers would be nicer if the environement is a pure
# AMQP one.
# what this client does is sending a message off to the queue 'test'
# where it expects a bot to send back a reply to the queue in the
# 'meta.reply_to' JSON structure. this client waits for the reply or
# times out after 60 seconds.

use strict;
use warnings;
use Net::AMQP::Simple;
use Data::UUID;
use JSON;

my $command = shift;

die "Usage: $0 [command]\n" unless $command;

my $uu   = Data::UUID->new;
my $amqp = Net::AMQP::Simple->new();

# the user
$amqp->user('guest');

# the password
$amqp->pass('guest');

# be chatty
$amqp->debug(1);

# create our JSON data structure that we use to communicate with our end
# points. this is only how i use my libs in my infrastructure, this is
# not mandatory, use whatever message format suites your needs.
my $hash = {
    meta => {
        reply_to => $uu->create_str(),
        logging  => 1,
        platform => 'iwmn',
        lang     => 'en',
        user     => 'iwmn',
    },
    data => { command => $command, },
};

# define a queue to send to
my $queue = "test";

# connect to the AMQP server
$amqp->connect();

# define the response queue and bind to it
$amqp->queue( $hash->{meta}->{reply_to}, 'autodelete' );

# push the message to the AMQP server and to the queue of your bot (test
# in our example)
$amqp->pub( to_json($hash), $queue );
my $done;

# loop till we get an answer
while ( !$done ) {

    # this poll will time out after 60 seconds
    check( $amqp->poll(60) );
}

# this only checks for valis JSON - not needed if you use your own non
# JSON based messaging format
sub check {
    foreach my $_req (@_) {
        my $req = from_json($_req);
        $done = 1;
        if ( $req->{error} ) {
            print STDERR $req->{error};
        }
        else {
            print $req->{result};
        }
    }
}

# kill the connection
$amqp->close();
