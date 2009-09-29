use inc::Module::Install;

name     'Net-AMQP-Simple';
all_from 'lib/Net/AMQP/Simple.pm';
author   q{Lenz Gschwendtner <norbu09@cpan.org>};
license  'perl';

requires 'Moose';
requires 'Net::AMQP';
requires 'File::ShareDir';
requires 'namespace::clean';

build_requires 'Test::More';

install_share  'share/Net-AMQP-Simple';
auto_install;

WriteAll;

