#!/usr/bin/perl -Ilib

use strict;
use warnings;
use Net::AMQP::Simple;

my $amqp = Net::AMQP::Simple->new();

$amqp->connect();
#print $amqp->close_channel();
#print $amqp->pub("helo world", "log");
print $amqp->queue("blubb");
$amqp->close();
