#!/usr/bin/env perl

use strict;
use warnings;

use Carp;
use Data::Dumper;
use English qw(-no_match_vars);
use File::Basename qw(basename);
use Getopt::Long qw(:config no_ignore_case);
use POSIX qw(:sys_wait_h);
use Proc::Daemon;
use Proc::PID::File;
use Pod::Usage;
use SQS::Queue::Worker;
use URI::Escape;

use Readonly;

Readonly our $DEFAULT_MAX_CHILDREN       => 5;
Readonly our $DEFAULT_VISIBILITY_TIMEOUT => 60;
Readonly our $DEFAULT_REDIS_PORT         => 6379;
Readonly our $DEFAULT_POLL_INTERVAL      => 2;
Readonly our $DEFAULT_MAX_SLEEP_PERIOD   => 60;

Readonly our $TRUE  => 1;
Readonly our $FALSE => 0;

Readonly our $SUCCESS => 0;
Readonly our $FAILURE => 1;

Readonly our $LOG4PERL_CONF => qq(
  log4perl.rootLogger = INFO, LOGFILE
  log4perl.appender.LOGFILE = Log::Log4perl::Appender::File
  log4perl.appender.LOGFILE.filename = %s
  log4perl.appender.LOGFILE.mode = append
  log4perl.appender.LOGFILE.layout = PatternLayout
  log4perl.appender.LOGFILE.layout.ConversionPattern = [%%d] %%p %%m%%n
);

our %PID_LIST;
our $VERSION = '@PACKAGE_VERSION@';

########################################################################
sub setup_signal_handlers {
########################################################################
  my ( $keep_going, $queue_handler ) = @_;

  $SIG{HUP} = sub {
    print {*STDERR} "Caught SIGHUP:  re-reading config file.\n";

    ${$keep_going} = $TRUE;

    # re-read config
    my $config = fetch_config( $queue_handler->get_config_file );
    $queue_handler->merge_config($config);

    return;
  };

  $SIG{INT} = sub {
    print {*STDERR} ("Caught SIGINT:  exiting gracefully\n");
    ${$keep_going} = $FALSE;
  };

  $SIG{QUIT} = sub {
    print {*STDERR} ("Caught SIGQUIT:  exiting gracefully\n");
    ${$keep_going} = $FALSE;
  };

  $SIG{TERM} = sub {
    print {*STDERR} ("Caught SIGTERM:  exiting gracefully\n");
    ${$keep_going} = $FALSE;
  };

  return;
}

########################################################################
sub set_reaper {
########################################################################
  my ($logger) = @_;

  # install *after* calling waitpid

  return $SIG{CHLD} = sub {
    while ( ( my $kid = waitpid -1, WNOHANG ) > 0 ) {
      delete $PID_LIST{$kid};

      $logger->debug(
        sub {
          return sprintf "reaping zombie process: [%s]\n%s", $kid, Dumper( [ pid_list => \%PID_LIST ] );
        }
      );
    }
  };
}

########################################################################
sub init_redis {
########################################################################
  my ($options) = @_;

  my ( $redis_server, $redis_port )
    = @{$options}{qw(redis_server redis_port)};

  return
    if !$redis_server;

  $redis_port //= $DEFAULT_REDIS_PORT;

  return eval {
    require Redis;

    my $server = sprintf '%s:%s', $redis_server, $redis_port;

    return Redis->new( server => $redis_server );
  }
}

########################################################################
sub require_module {
########################################################################
  my ($module) = @_;

  my $module_file = $module;
  $module_file =~ s/::/\//gxsm;
  $module_file = sprintf '%s.pm', $module_file;

  return require $module_file;
}

########################################################################
sub init_handler {
########################################################################
  my ($options) = @_;

  my ( $worker, $config_path )
    = @{$options}{qw(worker config)};

  $worker //= q{};
  $worker = ucfirst $worker;

  my $handler_class = $worker ? "SQS::Queue::Worker::$worker" : 'SQS::Queue::Worker';

  my $handler = eval {
    require_module $handler_class;

    return $handler_class->new( config_file => $config_path, %{$options} );
  };

  croak "could not instantiate worker: $handler_class\n$EVAL_ERROR"
    if !$handler || $EVAL_ERROR;

  return $handler;
}

########################################################################
sub usage {
########################################################################
  pod2usage(
    { -verbose => 1,
      -exitval => 1,
    }
  );

  return;
}

########################################################################
sub get_options {
########################################################################
  my %options;

  my @options_specs = qw(
    config|C=s
    endpoint-url|u=s
    help|h
    queue-url|q=s
    redis-port|p=i
    redis-server|R=s
    worker|w=s
    poll-interval|P=s
    visibility-timeout|V=i
    daemonize|D!
  );

  GetOptions( \%options, @options_specs )
    or usage();

  if ( $options{help} ) {
    usage();
  }

  foreach my $k ( keys %options ) {
    next if $k !~ /\-/xsm;

    my $v = $options{$k};
    $k =~ s/\-/_/gxsm;

    $options{$k} = $v;
  }

  return %options;
}

########################################################################
sub set_defaults {
########################################################################
  my ( $self, %defaults ) = @_;

  foreach ( keys %defaults ) {
    next if defined $self->get($_);
    $self->set( $_, $defaults{$_} );
  }

  return;
}

########################################################################
sub main {
########################################################################
  my %options = get_options();

  my $queue_handler = init_handler( \%options );

  set_defaults(
    $queue_handler,
    poll_interval      => $DEFAULT_POLL_INTERVAL,
    max_children       => $DEFAULT_MAX_CHILDREN,
    visibility_timeout => $DEFAULT_VISIBILITY_TIMEOUT,
    max_sleep_period   => $DEFAULT_MAX_SLEEP_PERIOD,
    daemonize          => $FALSE,
  );

  # if daemon, then log to file
  if ( $options{daemonize} ) {
    my $logfile       = sprintf '/var/log/sqs-%s', basename( $queue_handler->get_queue_url );
    my $log4perl_conf = sprintf $LOG4PERL_CONF, $logfile;

    $queue_handler->set_log4perl_conf( \$log4perl_conf );
  }

  my $redis = init_redis( \%options );

  # set up signal handlers
  my $keep_going = $TRUE;

  setup_signal_handlers( \$keep_going, $queue_handler );

  my $pid = Proc::Daemon::Init;

  if ( $pid && $options{daemonize} ) {
    print {*STDERR} sprintf 'daemon successfully launched with pid: %s', $pid;
    return 0;
  }

  $queue_handler->init();

  my $logger = $queue_handler->get_logger;

  my $queue_name = basename( $queue_handler->get_queue_url );

  my $pidfile = sprintf 'sqs-%s', $queue_name;

  # if already running, then exit
  if ( Proc::PID::File->running( name => $pidfile ) ) {
    # TODO check process id against process table
    print {*STDERR} sprintf 'handler for %s already running.', $queue_name;

    return $SUCCESS;
  }

  set_reaper($logger);
  set_defaults($queue_handler);

  my $max_children       = $queue_handler->get_max_children;
  my $visibility_timeout = $queue_handler->get_visibility_timeout;
  my $retry_timeout      = $queue_handler->get_retry_visibility_timeout;
  my $queue_url          = $queue_handler->get_queue_url;
  my $service            = $queue_handler->get_sqs_client;
  my $sleep              = $queue_handler->get_poll_interval;
  my $max_sleep_period   = $queue_handler->get_max_sleep_period;
  my $poll_interval      = $queue_handler->get_poll_interval;

  $logger->info( sprintf ' ------------------------------------' );
  $logger->info( sprintf ' Queue "%s" handler initialized with:', $queue_name );
  $logger->info( sprintf ' ------------------------------------' );
  $logger->info( sprintf ' Process ID.........[%d]', $PID );
  $logger->info( sprintf ' Queue name.........[%s]', $queue_name );
  $logger->info( sprintf ' SQS queue URL......[%s]', $queue_url );
  $logger->info( sprintf ' Max children.......[%d]', $max_children );
  $logger->info( sprintf ' Poll interval......[%d]', $poll_interval );
  $logger->info( sprintf ' Max sleep period...[%d]', $max_sleep_period );
  $logger->info( sprintf ' Idempotency........[%s]', $redis ? 'enabled' : 'disabled' );
  $logger->info( sprintf ' ------------------------------------' );

  my $message_id;

  while ($keep_going) {

    $logger->info( sprintf 'Reading queue...[%s]', $queue_name );

    my $message_list = $queue_handler->read_message();

    if ( $message_list && @{$message_list} ) {
      $logger->info( Dumper [$message_list] );

      my $message = $message_list->[0];

      # handle message
      $message_id = $message->{MessageId};

      $logger->info( sprintf 'id: %s', $message_id );

      # ignore message if duplicate and we are using Redis to enforce
      # idempotency of messages (SETNX = set if key does not exist)
      if ($redis) {
        if ( $redis->setnx( $message_id, $message->{Body} ) ) {
          $redis->expire( $message_id, $retry_timeout );
        }
        else {
          $logger->info( sprintf 'message %s already being handled', $message_id );
          next;
        }
      }

      $logger->info('Message received...dispatching...');

      # check to see if we have spawned the max number of children yet,
      # if so let's put this message on ice for a few seconds.
      if ( scalar( keys %PID_LIST ) >= $max_children ) {
        $logger->info( sprintf 'max number of children [%d] reached, rejecting message...', $max_children );

        $logger->info("changing timeout [$retry_timeout]");
        $queue_handler->change_message_visibility( $message, $retry_timeout );

        next;
      }

      my $pid = fork;

      if ( !defined $pid ) {
        $logger->error('Could not fork child!');
      }

      if ( $pid == 0 ) {

        eval {
          if ( $queue_handler->handler($message) ) {
            $logger->info('successfully processed message...deleting message');

            $queue_handler->delete_message($message);
          }
          else {
            $logger->error( sprintf 'message not handled: %s ', Dumper( [$message] ) );
          }
        };

        my $error = $EVAL_ERROR;

        if ($redis) {
          $redis->del($message_id);
        }

        if ($error) {
          my $message = sprintf 'processing error: %s', $error;

          $logger->error($message);
        }

        return $SUCCESS;
      }
      else {
        $PID_LIST{$pid} = scalar localtime;

        $sleep = $poll_interval;
      }
    }
    else {
      $logger->debug( sprintf 'No messages - sleeping for [%s] seconds...', $sleep );

      sleep $sleep;

      if ( $sleep < $max_sleep_period ) {
        $sleep += $poll_interval;
      }
    }
  }

  return $SUCCESS;
}

exit main();

1;

__END__

=pod

=head1 NAME

sqs-queue-processor.pl - harness for SQS queue handlers

=head1 SYNOPSIS

 sqs-queue-processor.pl Options

Example:

 sqs-queue-processor.pl --config-file=my-worker.cfg

=head1 OPTIONS

 --help, -h          this help message
 --redis-server, -R  Redis host if using Redis for idempotent message handling
 --redis-port, -p    Redis port (default: 6379)
 --config, -C        path to the config file (see note below)

=head1 DESCRIPTION

Implements a daemon that reads from AmazonE<039>s Simple Queue Service
(SQS) and dispatches messages to classes that are designed to handle
messages for that queue.

A queue handler is a class named according to the convention:

 SQS::Queue::Worker::{name}

Your handler class should subclass L<SQS:Queue::Worker>.

=head USING REDIS TO ENFORCE IDEMPOTENCY

In most cases, Amazon SQS will only deliver a message once. However,
Amazon SQS documentation includes this note:

I<Amazon SQS stores copies of your messages on multiple servers for
redundancy and high availability. On rare occasions, one of the
servers that stores a copy of a message might be unavailable when you
receive or delete a message.

If this occurs, the copy of the message isn't deleted on the server
that is unavailable, and you might get that message copy again when
you receive messages. Design your applications to be idempotent (they
should not be affected adversely when processing the same message more
than once).>

This script can use Redis to store a message id that is used to keep
track of messages received and in the process of being handled. This
will prevent a message from being processed more than once.

The script uses the SETNX method of Redis which will only set a key if
it does not exists and will return a true value if the key already
exists.

The key is set to the same timeout value as the visibility timeout of
the queue. This means that if the message is handled the message
should not be sent again and the key will timeout. If the message is
not handled, it will be sent again after the visibilty timeout is
reached which will coincide with the key timeout, allowing the message
to be handled.

B<TBD: Create a test for verifying proper handling of duplicate
messages.>

=head1 CONFIGURATION

=head1 SEE ALSO

L<Proc::Daemon>, L<Amazon::API::SQS>

=head1 AUTHOR

Rob Lauer - <rclauer@gmail.com>

=cut
