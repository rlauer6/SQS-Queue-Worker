package SQS::Queue::Worker;

# +------------------------------------------------+
# | (c) Copyright 2025, TBC DevelopmentGroup, LLC  |
# |              All Rights reserved.              |
# +------------------------------------------------+

use strict;
use warnings;

use Amazon::API::SQS;
use Carp;
use Data::Dumper;
use English qw(-no_match_vars);
use JSON;
use Log::Log4perl qw(:easy);
use Log::Log4perl::Level;

use Query::Param;

use Readonly;

Readonly::Scalar our $AMPERSAND => q{&};

Readonly::Scalar our $TRUE  => 1;
Readonly::Scalar our $FALSE => 0;

our $VERSION = '@PACKAGE_CONFIG';

__PACKAGE__->follow_best_practice;
__PACKAGE__->mk_accessors(
  qw(
    config_file
    endpoint_url
    last_message
    log4perl_conf
    log4perl_conf
    log_level
    logger
    max_children
    max_sleep_period
    poll_interval
    queue_url
    retry_visibility_timeout
    sqs_client
    visibility_timeout
    worker
  )
);

use parent qw(Class::Accessor::Fast Exporter);

our @EXPORT = qw(fetch_config);

########################################################################
sub fetch_config {
########################################################################
  my ($config_file) = @_;

  return
    if !$config_file;

  open my $fh, '<', $config_file
    or croak "could not open $config_file for reading\n";

  local $RS = undef;

  my $config = to_json(<$fh>);

  close $fh;

  return $config;
}

########################################################################
sub merge_config {
########################################################################
  my ( $self, $config ) = @_;

  foreach my $k ( keys %{$config} ) {
    $self->set( $k, $config->{$k} );
  }

  return;
}

########################################################################
sub new {
########################################################################
  my ( $class, @args ) = @_;

  my $options = ref $args[0] ? $args[0] : {@args};

  my $self = $class->SUPER::new($options);

  my $config = fetch_config( $self->get_config_file );
  $self->merge_config($config);

  croak "queue_url is a required option\n"
    if !$self->get_queue_url;

  return $self;
}

########################################################################
sub init {
########################################################################
  my ($self) = @_;

  $self->init_logger;

  $self->get_logger->info('logging started');

  my $sqs_client = Amazon::API::SQS->new(
    logger    => $self->get_logger,
    log_level => $self->get_log_level,
    url       => $self->get_endpoint_url
  );

  $self->set_sqs_client($sqs_client);

  return $self;
}

########################################################################
sub handler {
########################################################################
  my ( $self, $message ) = @_;

  print {*STDERR} Dumper($message);

  return $TRUE;
}

########################################################################
sub init_logger {
########################################################################
  my ($self) = @_;

  my $log_level = $self->get_log_level;
  $log_level //= 'info';

  $self->set_log_level( lc $log_level );

  my $log4perl_level = {
    trace => $TRACE,
    debug => $DEBUG,
    warn  => $WARN,
    info  => $INFO,
    error => $ERROR,
  }->{$log_level};

  $log4perl_level //= $INFO;

  # default config
  my $log4perl_config = $self->get_log4perl_conf;

  if ($log4perl_config) {
    Log::Log4perl->init($log4perl_config);
  }
  else {
    Log::Log4perl->easy_init( $log4perl_level // $INFO );
  }

  my $logger = Log::Log4perl->get_logger();

  if ($log4perl_level) {
    $logger->level($log4perl_level);
  }

  $self->set_logger($logger);

  return $self;
}

########################################################################
sub create_sqs_message {
########################################################################
  my ( $self, %options ) = @_;

  die "invalid message: no event key.\n"
    if !defined $options{event};

  return join $AMPERSAND, map { "$_=" . uri_escape( $options{$_} ) } keys %options;
}

########################################################################
sub parse_message {
########################################################################
  my ( $self, $message ) = @_;

  my $body = $message->getBody();

  $self->get_logger->debug( 'raw message: ' . $message->getBody() );

  my $params;

  # see if JSON string or CGI query string
  if ( $body =~ /\s*[{]/xsm ) {
    # looks like JSON...
    $params = eval { from_json($body); };
  }
  else {
    $params = eval { return Query::Param->new($body)->params(); };
  }

  if ($EVAL_ERROR) {
    $self->get_logger->error("error parsing message: $EVAL_ERROR");
  }

  return $params;
}

########################################################################
sub read_message {
########################################################################
  my ($self) = @_;

  my $sqs_client = $self->get_sqs_client;

  my $message = $sqs_client->ReceiveMessage( { QueueUrl => $self->get_queue_url, } );

  return
    if !keys %{$message};

  if ( ref $message ) {
    $message = $message->{Messages};
    $self->set_last_message($message);
  }

  return $message;
}

########################################################################
sub delete_message {
########################################################################
  my ( $self, $message ) = @_;

  return $self->get_sqs_client->DeleteMessage(
    { QueueUrl      => $self->get_queue_url,
      ReceiptHandle => $message->{ReceiptHandle},
    }
  );
}

########################################################################
sub change_message_visibility {
########################################################################
  my ( $self, $message, $timeout ) = @_;

  my $rsp = $self->get_sqs_client->ChangeMessageVisibility(
    { QueueUrl          => $self->get_queue_url,
      ReceiptHandle     => $message->{ReceiptHandle},
      VisibilityTimeout => $timeout
    }
  );

  $self->logger->debug( sprintf 'response: %s', $rsp );

  return $rsp;
}

########################################################################
sub DESTROY {
########################################################################
  my ($self) = @_;

  return;
}

1;

## no critic (RequirePodSections)

__END__

=pod

=head1 NAME

SQS::Queue

=head1 SYNOPSIS

 package SQS::Que
 
 use parent qw(TreasurersBriefcase::QueueHandler);
 
 my $queue_handler = TreasurersBriefcase::QueueHandler->new('briefcase');

 sub handler {
   ...
 }

=head1 DESCRIPTION

L<TreasurersBriefcase::QueueHandler> is a base class for writing
Amazon Simple Queue Service (SQS) message handlers.

=head1 NOTES

This base class is one element in a simple architecture, specific to
Treasurer's Briefcase, to handle messages from the SQS queue.  Follow
the cookbook below to implement additional handlers for Treasurer's
Briefcase workflow.

=over 5

=item 1. Create an SQS queue

Using the CLI or console create an SQS queue.  Name the queue something
related to the process that it designed for, using a prefix of
C<treasurersbriefcase->.  For example C<treasurersbriefcase-payment>.
The Amazon SQS creation process will assign the queue a URL - take note
of the new URL.

=item 2. Configure the queue

You can specify various default behaviors for the queue with regard to
the timing of messages on the AWS Management Console.

=over 5

=item * Visibility Timeout

This specifies the amount of time that the handler has to handle the
message before it becomes visible again on the queue.  When an SQS
message is read from the queue, SQS will lock the message and prevent
other handlers from reading it for some period of time that you define
(the default is 30s).

Once a handler is presented witht the message it needs to handle the
message, delete it from the queue or extend the visibility timeout
otherwise the message will become available to the next process that
reads from the queue.

=item * Delivery Delay

This parameter specifies the amount of time between message
deliveries.  If, for some reason you want to meter your handling of
messages, you can configure a delay that SQS will employ before the
next message becomes available.  This might be handly for example, if
you have some rogue program that is inadervantly bombarding your
application with messages.

=back

=item 3. Configure the queue in F<amazon-sqs.xml>

A configuration file is used to specify the AWS parameters including
queue definitions.  Add a new object for the queue in the body of
C<queues> object in the F<amazon-sqs.xml> file.  The configuration
file can be found in the build tree as F<~/config/amazon-sqs.xml.in>.
The configuration file is installed in the system directory:

 /etc/treasurersbriefcase/amazon

 <!-- Amazon Simple Queue Service -->
 <object name="queues">

   <object name="account">
     <scalar name="url">106518701080/treasurersbriefcase-account</scalar>
     <scalar name="visibility_timeout">60</scalar>
     <object name="log4perl">
       <scalar name="log4perl.appender.LOGFILE.layout.ConversionPattern">%H %d [%P] - (account) %F %L %c - %m%n</scalar>
     </object>
   </object>

 </object>

The important element here is the C<url> for the new queue (the one I
told you to take note of earlier).  Specify the path portion of the
URL using the C<url> item.  You can also override the visibility
timeout and the C<Log::Log4perl> parameters if you'd like - they
should match what you have configured on the console.

Note that the L<Log::Log4perl> appender property specifies a
pattern for the log message that includes the queue name.  This is
because the C<process-messages-daemon.pl> process opens up a log file
named F<tbc-sqs.log> and dumps messages to that file.

Each queue handler logs to the same file (initially) making it a
convenient spot to see that all of the messages are actually being
processed.  Each individual queue handler however, logs to its own
file in order to report status for handling of messages specific to
its own queue or even message.

Log files by queue as they are currently defined:

=over 5

=item treasurersbriefcase-briefcase

F</var/log/sqs-briefcase-process.log>

=item treasurersbriefcase-account

F</var/log/sqs-process-account.log>

=item treasurersbriefcase-payment

F</var/log/tbc-sqs.log>

=back

=item 4. Design your messages

Your applications should use the C<send_sqs_message()> method in the
C<amazon_web_services_lib> module to send messages to your queue.
Messages are expected to be formatted as CGI query string
arguments. At a minimum, the message should contain an C<event>
variable.

 event=process&account_id=1&key=spool/1/joe.pdf

The event variable should be interpreted by your C<handler()> method
to provide some functionality, so you need to decide what
funcdionality your queue is supporting and what information is
required. YMMV.

=item 5. Write your handler

You use the C<TreasurersBriefcase::QueueHandler> as your base class
and provide your own implemenation of the C<handler()> method.  The
suffix of your class name should be the same name as the queue as it
was specified in the C<queues> object of the F<amazon-sqs.xml>
configuration file with the first letter in upper case.  If your queue
was installed in the configuration file as C<briefcase> then you would
write a class called C<TreasurersBriefcase::QueueHandler::Briefcase>.

  package TreasurersBriefcase::QueueHandler::Briefcase;
    
  use parent qw/TreasurersBriefcase::QueueHandler/;
  
  sub handler {
    my ($self, $args) = @_;
    
    for ( $args->{event} ) {
  
      /process/ && do {
        return $self->event_process( %{$args} );
      };
  
      return 0;
    }
  
  }
  
  sub event_process {
    my ($self, %args) = @_;

    # do something useful
    return $TRUE;
  }

Your C<handler()> method should return a boolean value to indicate
whether the message was handled properly and can be deleted from the
queue.  Returning a false value will cause the message to re-appear in
the queue after the visibility timeout has elapsed.

Note that the handler shown above is just one implementation of how to
handle the events that your messages generate.  Conceivably you can
send messages that do not have an C<event> variable associated with
them and complete all of the functionality required in the
C<handler()> method and subsequently return a status code.

The pattern above is the pattern that the TreasurerE<039>s Briefcase
system will employ, so developers are encouraged to follow this pattern.

Name your event handler methods according to the event they are handling
using the naming convention shown below.

 event_{event-name}


=item 6. Daemonize your handler

The Perl script F<process-messages-daemon.pl> acts as a
driver for reading the queue and deliverying messages to your handler.
It does this by instantiating your Perl class based on the argument
C<--queue-name> passed to the driver.

 $ /usr/libexec/process-messages-daemon.pl --queue-name=briefcase

The F<process-message-daemon-.pl> Perl script is designed to be
invoked by a System V init script which is launched using the
F</sbin/service> program.

 $ sudo /sbin/service tbc-briefcased start

You should implement an C<init> script similar to the one below that
invokes the driver by calling C<bash> functions defined in
F<tbc-functions>.

A template has been created in this project called
F<tbc-initd.in> that you can use as a starting point.

  #!/bin/bash

  # chkconfig: 2345 99 1
  # description: TBC briefcase processing daemon
  # processname: tbc-briefcased
  # pidfile: /var/run/tbc-sqs-briefcase.pid

  . /etc/rc.d/init.d/functions

  . /etc/rc.d/init.d/tbc-functions

  tbc_script="/usr/libexec/process-messages-daemon.pl --queue=briefcase"
  pidfile=/var/run/tbc-sqs-briefcase.pid
  prog=tbc-briefcased

  case "$1" in 
      start)
  	start
  	;;

      stop)
  	stop
  	;;

      restart)
  	restart
  	;;

      graceful)
  	graceful
  	;;

      status)
  	status -p ${pidfile}
  	RETVAL=$?
  	;;

      *)
  	echo $"Usage: $prog {start|stop|restart}"
  esac

=back

=head1 METHODS AND SUBROUTINES

=head2 new

 new( [queue-name] )

Since this class is typically used as the parent class for your queue
handling class, the assumption is that the suffix of your class name
is the name of the queue as represented in the configuration file.  In
other words if my class is C<TreasurersBriefcase::QueueHandler::Foo>,
then my queue name is 'foo' and there exists a 'foo' configuration
object in the F<amazon-sqs.xml> file.

=head1 LOGGING METHODS

=head2 init_logger

Initializes a C<Log::Log4perl> log file.

=head2 logger

Returns the current logging object.

=head2 mail_logger

Returns a logger that will send an email message.

=head1 DATABASE METHODS

Messages from your events typically will include an C<account_id>
value if the message is specific to an account. Use these methods to
open the master or account databases.

 sub event_foo {
    my ($self, %args) = @_;

    my $dbi = $self->open_master_database;
    my $account_dbi = $self->open_acccount_database($args{account_id});
 }

Once opened, you can access the database handles using the getter methods.

  my $dbi = $self->master_dbi;
  my $account_dbi = $self->account_dbi;

=head2 open_master_database

 open_master_database()

=head2 open_account_database

 open_account_database( account-id )

Opens and sets the current account database connection.

=head2 account_dbi

 account_dbi

Returns the L<DBI> handle for an account database.

=head2 master_dbi

 master_dbi

Returns the L<DBI> handle for the master database.  Opens the
database if it has not yet been opened.

=head1 QUEUE METHODS

=head2 queue_url

Returns the current queue URL.

=head2 queue_config

Sames as C<get_queue()>

=head2 get_queue

 my $qcfg = get_queue();

Returns the configuration of the currently set queue.

=head2 set_queue

 set_queue( queue-name)

Sets the current queue to the name specified.  The name should
correspond to a key in the F<amazon-sqs.xml> configuration file.

Returns the current queue configuration as a hash ref.

=head2 poll_interval

 poll_interval()

=head2 max_sleep_period

Returns the max sleep period.  The system will increment the sleep period until this value.

=head2 queue

=head2 set_queue_name

=head2 get_queue_name

Set or return the current queue name.

=head2 create_sqs_message

 create_sqs_message( options )

Creates a formatted message that can be sent to an SQS queue.

C<options> is a hash of key/value pairs specific to the message. You
should at least have an C<event> key defined.

Example:

 create_sqs_message( event => 'process', key => 'spool/52/foo.jpg', account => 52);

=head2 delete_message

 delete_message( message )

Deletes an SQS message from the queue.

=head2 parse_message

 parse_message()

Returns a hash reference containing the parameters passed in the
message.  Message parameters are passed as CGI query strings or a JSON
object.

=head2 read_message

 read_message()

Returns an SQS message body or C<undef>.

=head2 get_last_message

 get_last_message()

Returns the last message read.

=head2 send_message

 send_message( message );

Sends a message on the current current queue.

=head2 change_message_visibility

 change_message_visibility( message, timeout );

Changes the message visibility so that the message will not be
available for a given number of seconds.  We typically use this when
we have reached the maxium number of messages we want to process at
one time for a queue and want to tell delay the availabity of this
message for some time.

=head1 MISCELLANEOUS METHODS

=head2 email_message

See L<email_message|tbc_utils_lib/email_message>.

=head2 get_amazon_config

 get_amazon_config()

=head2 get_primary_contact

 get_primary_contact( account_dbh => handle, dbh => handle, account_id => id )

Returns the primary contact information for an account. See L<TreasurersBriefcase::Contact>.

=head2 handler

 handler

You should implement a handler.

=head2 helpers

  helpers()

Returns the F<helpers.xml> configuration object that contains
pathnames for helper programs and scripts. See L<TreasurersBriefcase::Helpers>.

=head1 SEE OTHER

L<Amazon::SQS::Client>, L<tbc_daemon_utils>

=head1 AUTHOR

Rob Lauer - <rlauer6@comcast.net>

=cut
