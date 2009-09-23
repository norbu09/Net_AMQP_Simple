#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Net::AMQP::Simple' );
}

diag( "Testing Net::AMQP::Simple $Net::AMQP::Simple::VERSION, Perl $], $^X" );
