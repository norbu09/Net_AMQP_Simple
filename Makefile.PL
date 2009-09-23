use inc::Module::Install;

name     'Net-AMQP-Simple';
all_from 'lib/Net/AMQP/Simple.pm';
author   q{Lenz Gschwendtner <norbu09@cpan.org>};
license  'perl';

build_requires 'Test::More';

auto_install;

WriteAll;

