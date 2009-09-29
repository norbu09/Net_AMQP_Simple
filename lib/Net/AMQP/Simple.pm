package Net::AMQP::Simple;

use Moose;
use namespace::clean -except => ['meta'];

#use Params::Validate qw(validate validate_with);
use IO::Socket::INET;
use Net::AMQP;
use Net::AMQP::Common qw(:all);
use File::ShareDir 'dist_file';
use Carp;
use Data::Dumper;

our $VERSION = '0.02';

has 'host' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    default  => sub { '127.0.0.1' }
);

has 'port' => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => sub { 5672 }
);

has 'user' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    default  => sub { 'guest' }
);

has 'pass' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    default  => sub { 'guest' }
);

has 'vhost' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    default  => sub { '/' }
);

has 'channel' => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => sub { 0 }
);

has 'remote' => (
    is       => 'rw',
    clearer  => 'close_socket',
    required => 0,
);

has 'debug' => (
    is       => 'rw',
    isa      => 'Int',
    required => 0,
);

has 'reading' => (
    is        => 'rw',
    clearer   => 'stop_reading',
    predicate => 'is_reading',
);

has 'wait_synchronous' => (
    is        => 'rw',
    isa       => 'HashRef',
    clearer   => 'clear_waiting',
    predicate => 'has_waiting',
    default   => sub { {} },
);

has 'channels' => (
    is      => 'rw',
    isa     => 'HashRef',
    clearer => 'clear_channels',
    default => sub { {} },
);

sub connect {
    my ($self) = @_;

    my $file = File::ShareDir::dist_file( 'Net-AMQP-Simple', 'amqp0-8.xml' );
    Net::AMQP::Protocol->load_xml_spec($file);

    my $sock = IO::Socket::INET->new(
        Proto    => "tcp",
        PeerAddr => $self->host,
        PeerPort => $self->port,
    ) or confess("Error connecting to AMQP Server!");

    print STDERR "connected to " . $self->host . "...\n" if $self->debug;
    print $sock Net::AMQP::Protocol->header;
    $self->remote($sock);
    $self->reading(1);
    $self->_sync_read();
}

sub open_channel {
    my ( $self, $id ) = @_;

    my $channels = $self->channels;
    if ( $id && $channels->{$id} ) {
        confess "Channel id $id is already in use";
    }
    elsif ( !$id ) {
        foreach my $i ( 1 .. ( 2**16 - 1 ) ) {
            if ( !$channels->{$i} ) {
                $id = $i;
                last;
            }
        }
        confess "Ran out of channel ids (!!)" unless $id;
    }
    $channels->{$id} = $$;
    $self->channels($channels);
    my $frame =
      Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Channel::Open->new(), );
    $self->_post($frame, $id);
    my @frames = $self->_read();
    foreach my $frame (@frames) {
        return $id
          if $frame->method_frame->isa('Net::AMQP::Protocol::Channel::OpenOk');
    }
    confess "Did not get a Channel!";
}

sub close_channel {
    my ( $self, $id ) = @_;

    my $err;
    if ($id) {

        my $frame =
          Net::AMQP::Frame::Method->new(
            method_frame => Net::AMQP::Protocol::Channel::Close->new(), );
        $self->_post($frame, $id);
      FRAME:
        while ( my @frames = $self->_read() ) {
            foreach my $frame (@frames) {
                if ( $frame->isa('Net::AMQP::Frame::Method') ) {
                    my $method = $frame->method_frame;
                    if (
                        ref($method) eq
                        'Net::AMQP::Protocol::Channel::CloseOk' )
                    {
                        last FRAME;
                    }
                    elsif (
                        ref($method) eq 'Net::AMQP::Protocol::Basic::Return' )
                    {
                        $err = "MESSAGE ERROR - " . $method->reply_text;
                    }
                    elsif (
                        ref($method) eq
                        'Net::AMQP::Protocol::Connection::Close' )
                    {
                        $err = $method->reply_text;
                    }
                }
            }
        }
    }
    else {
        $err = "ID ERROR - Expected Channel ID";
    }
    return $err;
}

sub pub {
    my ( $self, $message, $queue, $exchange ) = @_;

    my %method_opts = (
        ticket      => 0,
        exchange    => $exchange,    # default exchange
        routing_key => $queue,       # route to my queue
        mandatory   => 1,
        immediate   => 0,
    );

    my %content_opts = (
        content_type     => 'application/octet-stream',
        content_encoding => '',
        headers          => {},
        delivery_mode    => 1,                            # non-persistent
        priority         => 1,
        correlation_id   => '',
        reply_to         => '',
        expiration       => '',
        message_id       => '',
        timestamp        => time,
        type             => '',
        user_id          => '',
        app_id           => '',
        cluster_id       => '',
    );

    my $id    = $self->open_channel();
    my $frame = Net::AMQP::Protocol::Basic::Publish->new(%method_opts);
    $self->_post($frame, $id);

    $frame = Net::AMQP::Frame::Header->new(
        weight    => 0,
        body_size => length($message),
        header_frame =>
          Net::AMQP::Protocol::Basic::ContentHeader->new(%content_opts),
    );
    $self->_post($frame, $id);

    $frame = Net::AMQP::Frame::Body->new( payload => $message );
    $self->_post($frame, $id);

    return $self->close_channel($id);
}

sub queue {
    my ( $self, $queue, $auto ) = @_;

    my %opts = (
        ticket       => 0,
        queue        => $queue,
        consumer_tag => '',                 # auto-generated
        no_ack       => 1,
        exclusive    => 0,
        auto_delete  => ( $auto ? 1 : 0 ),
        nowait       => 0,                  # do not send the ConsumeOk response
    );

    my $id = $self->open_channel();

    my $frame =
      Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Queue::Declare->new(%opts) );
    $self->_post($frame, $id);

    $frame =
      Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Basic::Consume->new(%opts) );
    $self->_post($frame, $id);
    my $err;
  FRAME:
    while ( my @frames = $self->_read() ) {
        foreach my $frame (@frames) {
            if ( $frame->isa('Net::AMQP::Frame::Method') ) {
                my $method = $frame->method_frame;
                if ( ref($method) eq 'Net::AMQP::Protocol::Basic::ConsumeOk' ) {
                    last FRAME;
                }
                elsif (
                    ref($method) eq 'Net::AMQP::Protocol::Connection::Close' )
                {
                    warn $method->reply_text;
                }
            }
        }
    }
    return $id;
}

sub poll {
    my ( $self, $id, $timeout) = @_;

    if($timeout){
        local $SIG{ALRM} = sub  { return "timeout" };
        alarm $timeout;
    }
    my @result;
    my @frames = $self->_read();
    foreach my $frame (@frames) {
        if ( $frame->isa('Net::AMQP::Frame::Body') ) {
            push( @result, $frame->{payload} );
        }
    }
    if($timeout){
        alarm 0;
    }
    return @result;
}

sub close {
    my ( $self ) = @_;

    my $frame = Net::AMQP::Frame::Method->new(
        method_frame => Net::AMQP::Protocol::Connection::Close->new() );
    $self->_post($frame);
    $self->clear_channels;

  FRAME:
    while ( my @frames = $self->_read() ) {
        foreach my $frame (@frames) {
            if ( $frame->isa('Net::AMQP::Frame::Method') ) {
                my $method = $frame->method_frame;
                if ( ref($method) eq 'Net::AMQP::Protocol::Connection::CloseOk' ) {
                    last FRAME;
                }
            }
        }
    }

    my $sock = $self->remote;
    close $sock;
    $self->close_socket;

    return;
}

############ internal API ###########

sub _sync_read {
    my ($self) = @_;

    my @frames;
    while ( $self->is_reading ) {
        push( @frames, $self->_parser( $self->_read() ) );
    }
    return @frames;
}

sub _post {
    my ( $self, $output, $channel, $sync ) = @_;

    my $remote = $self->remote;

    if ( $output->isa("Net::AMQP::Protocol::Base") ) {
        $output = $output->frame_wrap;
    }
    $output->channel($channel || 0);

    print STDERR "--> " . Dumper($output) . "\n" if $self->debug;
    print $remote $output->to_raw_frame();

    if ($sync) {
        my $output_class = ref( $output->method_frame );
        my $responses    = $output_class->method_spec->{responses};
        $self->{wait_synchronous}{$output_class} = {
            request       => $output,
            responses     => $responses,
            process_after => [],
        };
    }

}

sub _read {
    my ($self) = @_;
    my $data;
    my $stack;

    # read lentgh (in Bytes)
    read( $self->remote, $data, 8 );
    $stack .= $data;
    my ( $type_id, $channel, $length ) = unpack 'CnN', substr $data, 0, 7, '';

    # read until $length bytes read
    while ( $length > 0 ) {
        $length -= read( $self->remote, $data, $length );
        $stack .= $data;
    }

    my @frames = Net::AMQP->parse_raw_frames( \$stack );
    print STDERR "<-- " . Dumper(@frames) if $self->debug;
    print STDERR "-----------\n" if $self->debug;
    return @frames;
}

sub _parser {
    my ( $self, @frames ) = @_;

  FRAMES:
    foreach my $frame (@frames) {

        if ( $frame->isa('Net::AMQP::Frame::Method') ) {
            my $method_frame = $frame->method_frame;

# Check the 'wait_synchronous' hash to see if this response is a synchronous reply
            my $method_frame_class = ref $method_frame;
            if ( $method_frame_class->method_spec->{synchronous} ) {
                print STDERR "Checking 'wait_synchronous' hash against $method_frame_class\n"
                  if $self->debug;

                my $matching_output_class;
                my $waiting = $self->wait_synchronous;
                while ( my ( $output_class, $details ) = each %{$waiting} ) {
                    next unless $details->{responses}{$method_frame_class};
                    $matching_output_class = $output_class;
                    last;
                }

                if ($matching_output_class) {
                    print STDERR 'Response type '
                      . $method_frame_class
                      . ' found from waiting request '
                      . $matching_output_class . " \n"
                      if $self->debug;

                    my $details = delete $waiting->{matching_output_class};
                    $self->wait_synchronous($waiting);

                    # Call the asynch callback if there is one
                    if ( my $callback =
                        delete $details->{request}{synchronous_callback} )
                    {
                        print STDERR "Calling $matching_output_class callback\n"
                          if $self->debug;
                        $callback->($frame);
                    }

                    # Dequeue anything that was blocked by this
                    foreach my $output ( @{ $details->{process_after} } ) {
                        print STDERR  
"Dequeueing items that blocked due to $method_frame_class\n"
                          if $self->debug;
                        $self->_post( $output,0,1 );
                    }

                    # Consider this frame handled
                    $self->stop_reading;
                    next FRAMES;
                }
            }

            # Act upon connection-level methods
            if ( $frame->channel == 0 ) {
                if (
                    $method_frame->isa(
                        'Net::AMQP::Protocol::Connection::Start')
                  )
                {
                    $self->_post(
                        Net::AMQP::Protocol::Connection::StartOk->new(
                            client_properties => {
                                platform    => 'Perl',
                                product     => __PACKAGE__,
                                information => 'http://github.com/norbu09/',
                                version     => $VERSION,
                            },
                            mechanism => 'AMQPLAIN'
                            , # TODO - ensure this is in $method_frame{mechanisms}
                            response =>
                              { LOGIN => $self->user, PASSWORD => $self->pass },
                            locale => 'en_US',
                        ),
                        ,0,1);
                    next FRAMES;
                }
                elsif (
                    $method_frame->isa('Net::AMQP::Protocol::Connection::Tune')
                  )
                {
                    $self->_post(
                        Net::AMQP::Protocol::Connection::TuneOk->new(
                            channel_max => 0,
                            frame_max   => 131072
                            , # TODO - actually act on this number and the Tune value
                            heartbeat => 0,
                        ),
                        0,1
                    );
                    $self->_post(
                        Net::AMQP::Frame::Method->new(
                            method_frame =>
                              Net::AMQP::Protocol::Connection::Open->new(
                                virtual_host => $self->vhost,
                                capabilities => '',
                                insist       => 1,
                              ),
                        ),
                        ,
                        0,1
                    );
                    next FRAMES;
                }
            }
        }

        if ( $frame->channel != 0 ) {
            my $channel = $self->{channels}{ $frame->channel };
            if ( !$channel ) {

#$self->{Logger}->error("Received frame on channel ".$frame->channel." which we didn't request the creation of");
                next FRAMES;
            }
            #$self->_post( $channel->{Alias}, server_input => $frame );
        }
        else {

            #$self->{Logger}->error("Unhandled input frame ".ref($frame));
        }
    }
}

=head1 NAME

Net::AMQP::Simple - The great new Net::AMQP::Simple!

=head1 VERSION

Version 0.01


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Net::AMQP::Simple;

    my $foo = Net::AMQP::Simple->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 FUNCTIONS

=head2 function1

=head1 AUTHOR

Lenz Gschwendtner, C<< <norbu09 at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-net-amqp-simple at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Net-AMQP-Simple>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Net::AMQP::Simple


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Net-AMQP-Simple>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Net-AMQP-Simple>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Net-AMQP-Simple>

=item * Search CPAN

L<http://search.cpan.org/dist/Net-AMQP-Simple/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2009 Lenz Gschwendtner.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;    # End of Net::AMQP::Simple
