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
use SQS::Queue::Constants qw(:all);
use URI::Escape;

our %PID_LIST;

our $VERSION = $SQS::Queue::Worker::Constants::VERSION;

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
    daemonize|D!
    endpoint-url|u=s
    help|h
    log-path|L
    log-level|l
    max-children|m=s
    max-sleep-period|M=i
    poll-interval|P=s
    queue-url|q=s
    redis-port|p=i
    redis-server|R=s
    visibility-timeout|V=i
    worker|w=s
  );

  GetOptions( \%options, @options_specs )
    or usage();

  if ( $options{help} ) {
    usage();
  }

  foreach my $k ( keys %options ) {
    next if $k !~ /\-/xsm;

    my $v = delete $options{$k};
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

  my $config = eval {
    return {}
      if !$options{config};

    return fetch_config( $options{config} );
  };

  my $queue_handler = init_handler( \%options );

  set_defaults(
    $queue_handler,
    poll_interval      => $DEFAULT_POLL_INTERVAL,
    max_children       => $DEFAULT_MAX_CHILDREN,
    visibility_timeout => $DEFAULT_VISIBILITY_TIMEOUT,
    max_sleep_period   => $DEFAULT_MAX_SLEEP_PERIOD,
    daemonize          => $FALSE,
    log_path           => $DEFAULT_LOG_PATH,
    %{$config}

  );

  my $logfile;

  # if daemon, then log to file
  if ( $options{daemonize} && !$config->{log4perl_conf} ) {
    $logfile = sprintf '%s/sqs-%s', $queue_handler->get_log_path, basename( $queue_handler->get_queue_url );
    my $log4perl_conf = sprintf $LOG4PERL_CONF, $logfile;

    $queue_handler->set_log4perl_conf( \$log4perl_conf );
  }

  my $redis = init_redis( \%options );

  # set up signal handlers
  my $keep_going = $TRUE;

  setup_signal_handlers( \$keep_going, $queue_handler );

  my $queue_name = basename( $queue_handler->get_queue_url );

  my $pidfile = sprintf 'sqs-%s', $queue_name;

  if ( $options{daemonize} && Proc::PID::File->running( name => $pidfile ) ) {
    # TODO check process id against process table
    print {*STDERR} sprintf 'handler for %s already running.', $queue_name;

    return $SUCCESS;
  }

  if ( $options{daemonize} ) {
    my $pid = Proc::Daemon::Init;

    if ($pid) {
      print {*STDERR} sprintf "daemon successfully launched with pid: %s\n", $pid;
      return $SUCCESS;
    }
  }

  $queue_handler->init();

  my $logger = $queue_handler->get_logger;

  # if the user provided a log4perl configuration we may not know the
  # logfile name yet
  if ( $options{daemonize} && !$logfile ) {
    my $appender = $logger->appender_by_name('LOGFILE');

    if ($appender) {
      $logfile = $appender->{filename};
    }
  }

  # when we forked, the Proc::Daemon::Init closed STDERR, STDOUT, so
  # we need to reopen STDERR if we want error messages to end up in
  # the logs
  if ( $options{daemonize} && $logfile ) {
    open STDERR, '>>', $logfile;
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
  $logger->info( sprintf ' Version............[%s]', $VERSION );
  $logger->info( sprintf ' Process ID.........[%d]', $PID );
  $logger->info( sprintf ' Queue name.........[%s]', $queue_name );
  $logger->info( sprintf ' Worker.............[%s]', $queue_handler->get_worker );
  $logger->info( sprintf ' SQS queue URL......[%s]', $queue_url );
  $logger->info( sprintf ' Max children.......[%d]', $max_children );
  $logger->info( sprintf ' Poll interval......[%d]', $poll_interval );
  $logger->info( sprintf ' Max sleep period...[%d]', $max_sleep_period );
  $logger->info( sprintf ' Idempotency........[%s]', $redis ? 'enabled' : 'disabled' );
  $logger->info( sprintf ' ------------------------------------' );

  my $message_id;

  while ($keep_going) {

    $logger->info( sprintf 'Reading queue [%s]...next read in [%s] seconds', $queue_name, $sleep );

    my $message = $queue_handler->read_message();

    if ($message) {
      # handle message
      $message_id = $queue_handler->get_message_id;

      $logger->info( sprintf 'Message [%s] received...dispatching...', $message_id );
      $logger->debug( Dumper [$message] );

      # ignore message if duplicate and we are using Redis to enforce
      # idempotency of messages (SETNX = set if key does not exist)
      if ($redis) {
        if ( $redis->setnx( $message_id, $queue_handler->get_body ) ) {
          $redis->expire( $message_id, $retry_timeout );
        }
        else {
          $logger->info( sprintf 'message %s already being handled', $message_id );
          next;
        }
      }

      # check to see if we have spawned the max number of children yet,
      # if so let's put this message on ice for a few seconds.
      if ( scalar( keys %PID_LIST ) >= $max_children ) {
        $logger->info( sprintf 'max number of children [%d] reached, rejecting message...', $max_children );
        $logger->info("changing timeout to: [$retry_timeout]");

        $queue_handler->change_message_visibility($retry_timeout);

        next;
      }

      my $pid = fork;

      if ( !defined $pid ) {
        $logger->error('Could not fork child!');
      }

      if ( $pid == 0 ) {

        eval {
          if ( $queue_handler->handler() ) {
            $logger->info( 'successfully processed message...deleting message', $message_id );

            $queue_handler->delete_message();
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

=head1 USAGE

 sqs-queue-processor.pl Options

Example:

 sqs-queue-processor.pl --config-file=my-worker.cfg

=head2 Options

 --config, -C             configuration file
 --config, -C             path to the config file (see note below)
 --daemonize, -D          daemonize the process
 --endpoint-url, -u       endpoint URL if using a mocking service like LocalStack
 --help, -h               this help message
 --max-children, -m       maximum number of simultaneous workers that will be forked, default: 5
 --max-sleep-period, -M   maximum amount of time process will sleep when no messages are present, default: 30
 --poll-interval, -P      polling interval in seconds, default: 2
 --queue-url, -q          the AWS queue url
 --redis-port, -p         Redis port (default: 6379)
 --redis-server, -R       Redis host if using Redis for idempotent message handling (optional)
 --visibility-timeout, V  number of seconds before a message becomes available again, default: 60
 --worker, -w             suffix of the worker module, e.g. MyWorker

=head2 Notes

=over 5

=item 1. Worker classes should be name SQS::Queue::Worker::{worker-name}

=item 2. The default log file when you daemonize the process will is /var/log/sqs-{queue-name}.log

=item 3. If no messages are on the queue, the process will sleep for
the poll interval time. If no messages are found after sleeping, then
the sleep time is incremented by the poll interval amount until
max-sleep-period is reached. After a message is received the sleep
time is reset to the poll interval time.

=item 4. If the max children have been spawned, messages are put back
on the queue by resetting their visibility timeout value.

=item 5. If no worker is defined, the default worker will log messages
and delete them from the queue

=item 6. Any of the options can be placed in a configuration file
which should in JSON format. Underscores in place of '-'
in your JSON file.

=item 7. Amazon credentials are discovered for you using the
Amazon::Credentials module

=item 8. See perldoc SQS::Queue::Workder for more details.

=item 9. The initial visibility timeout was determined when you
created the queue. If you reject the message by returning a false
value from your handler, the message will appear again after that
timeout. If in your handler you want to change that value, use the
C<change_message_visibility> method and return a false value.

=back

=head1 DESCRIPTION

Implements a daemon that reads from AmazonE<039>s Simple Queue Service
(SQS) and dispatches messages to classes that are designed to handle
messages for that queue.

A queue handler is a class named according to the convention:

 SQS::Queue::Worker::{name}

Your handler class should subclass L<SQS:Queue::Worker> and implement
at least a C<handler()> method. The method will receive the message as
hash reference.

    {
    'ReceiptHandle' => 'NDE1NmMxY2ItYzJjMi00MzUxLTllYTgtZDk2NTJiODQ2NjhjIGFybjphd3M6c3FzOnVzLWVhc3QtMTowMDAwMDAwMDAwMDA6ZnVtYW5xdWV1ZSA0MTcwOTg3OS0wMTIyLTRmZTgtYTJiYS05MTY0ZGEyZDk3ODMgMTc1MDE5MjQzMi41Njc4NTQ=',
    'MD5OfBody' => 'acbd18db4cc2f85cedef654fccc4a4d8',
    'MessageId' => '41709879-0122-4fe8-a2ba-9164da2d9783',
    'Body' => 'foo'
  }

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

=head1 DAEMONIZING YOUR HANDLER

Your handler can run from the terminal or you can daemonize the
process using the C<--daemonize> option at startup. When you daemonize
the handler, logs are written to C</var/log/sqs-{queue-name}> by
default so you probably want to run this process as root. If you don't
want to run as root and want to daemonize the process, set the log
path using the C<--log-path> option.

 sudo sqs-queue-processor.pl --config /etc/sqs-queue-processor/fumanque --daemonize

=head1 SEE ALSO

L<Proc::Daemon>, L<Amazon::API::SQS>

=head1 AUTHOR

Rob Lauer - <rclauer@gmail.com>

=cut
