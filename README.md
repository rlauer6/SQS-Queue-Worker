# NAME

SQS::Queue::Worker - parent class for queue processing handlers.

# SYNOPSIS

    package SQS::Queue::Worker::FumanQueue
    
    use parent qw(SQS::Queue::Worker);
    
    sub handler {
      my ($self) = @_;

      my $message = $self->get_last_message();
      my $body = $self->get_body;
      
      # or
      $self->parse_message();
      my $params = $self->get_message_params();

      return 1;
    }

    1;

# DESCRIPTION

[SQS::Queue::Worker](https://metacpan.org/pod/SQS%3A%3AQueue%3A%3AWorker) is a base class for writing Amazon Simple Queue
Service (SQS) message handlers.

# VERSION

This documentation refers to version 1.0.3.

# NOTES

This base class is one element in a simple architecture to handle
messages from the SQS queue. Follow the cookbook below to implement
handlers for your workflow.

- Create an SQS queue

    Using the CLI or console create an SQS queue.

- Configure the queue

    You can specify various default behaviors for the queue with regard to
    the timing of messages on the AWS Management Console.

    - Visibility Timeout

        This specifies the amount of time that the handler has to handle the
        message before it becomes visible again on the queue.  When an SQS
        message is read from the queue, SQS will lock the message and prevent
        other handlers from reading it for some period of time that you define
        (the default is 30s).

        Once a handler is presented witht the message it needs to handle the
        message, delete it from the queue or extend the visibility timeout
        otherwise the message will become available to the next process that
        reads from the queue.

    - Delivery Delay

        This parameter specifies the amount of time between message
        deliveries.  If, for some reason you want to meter your handling of
        messages, you can configure a delay that SQS will employ before the
        next message becomes available.  This might be handly for example, if
        you have some rogue program that is inadervantly bombarding your
        application with messages.

- Create a configuration file

    A configuration file can be used to specify the AWS parameters
    including queue definitions. It can be in YAML or JSON format.

        ---
        queue_url: http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/fumanqueue
        log_path: /tmp
        visibility_timeout: 20
        max_children: 3
        poll_interval: 1
        worker: FumanQueue
        log_level: info
        log4perl_conf:
          - log4perl.rootLogger = INFO, LOGFILE
          - log4perl.appender.LOGFILE = Log::Log4perl::Appender::File
          - log4perl.appender.LOGFILE.filename = /var/log/sqs-fumanqueue
          - log4perl.appender.LOGFILE.mode = append
          - log4perl.appender.LOGFILE.layout = PatternLayout
          - log4perl.appender.LOGFILE.layout.ConversionPattern = [%d] %p %m%n

    The important element here is the `queue_url`.  You can also override
    the visibility timeout and the `Log::Log4perl` parameters if you'd
    like - they should match what you have configured on the console.

- Design your messages

    Messages are expected to be formatted as query string arguments or
    JSON payloads. Use the `parse_message()` method to create a hash of
    the message payload. If you don't want to use query string parameters
    or a JSON payload you can get the message body using the `get_body()`
    method.

- Write your handler

    You use the `SQS::Queue::Worker` as your base class and provide your
    own implemenation of the `handler()` method. See
    ["SYNOPSIS"](#synopsis). Handler classes should be named using the convention:

        SQS::Queue::Worker::{worker-name}

    Example: SQS::Queue::Worker::FumanQueue

    Your `handler()` method should return a boolean value to indicate
    whether the message was handled properly and can be deleted from the
    queue. Returning a false value will cause the message to re-appear in
    the queue after the visibility timeout has elapsed.

- Daemonize your handler

    The Perl script `sqs-queue-processor.pl` acts as a
    driver for reading the queue and deliverying messages to your handler.
    It does this by instantiating your Perl class based on the argument
    `worker` value in your configuration.

        sudo sqs-queue-processor.pl --config /etc/sqs-fumanqueue.yml

# LONG POLLING

By default, the script will read messages from the SQS queue using
long polling. You can configure the wait time by setting the
`wait_time_seconds` parameter in the configuration file. If you want
to disable long polling, set the `wait_time_seconds` to 0.

default: 20s

# METHODS AND SUBROUTINES

## new

    new()

# LOGGING METHODS

## init\_logger

Initializes a `Log::Log4perl` log file.

## get\_logger

Returns the current logging object.

# QUEUE METHODS

## get\_queue\_url, set\_queue\_url

Returns the current queue URL.

## get\_poll\_interval, set\_poll\_interval

    get_poll_interval()

## get\_max\_sleep\_period, set\_max\_sleep\_period

Returns the max sleep period.  The system will increment the sleep period until this value.

## get\_visibility\_timeout, set\_visibility\_timeout

## create\_sqs\_message

    create_sqs_message( options )

Creates a formatted message that can be sent to an SQS queue.

`options` is a hash of key/value pairs specific to the message. You
should at least have an `event` key defined.

Example:

    my $payload = create_sqs_message( event => 'process', key => 'spool/52/foo.jpg', account => 52);

_This method can also be called as a class method._

    use SQS::Queue::Worker qw(crate_sqs_message);

    create_sqs_message(message-body)

If you pass a reference it will be formatted as a JSON string.

## delete\_message

    delete_message( message )

Deletes an SQS message from the queue.

## parse\_message

    parse_message()

Returns a hash reference containing the parameters passed in the
message. Message parameters are passed as CGI query strings or a JSON
object.

_This method can also be called as a class method._

    use SQS::Queue::Worker qw(parse_message);

    parse_message(message-body)

## read\_message

    read_message()

Returns an SQS message body or `undef`.

## get\_last\_message

    get_last_message()

Returns the last message read.

## send\_message

    send_message( message );

Sends a message on the current current queue.

## change\_message\_visibility

    change_message_visibility( message, timeout );

Changes the message visibility so that the message will not be
available for a given number of seconds.  We typically use this when
we have reached the maxium number of messages we want to process at
one time for a queue and want to delay the availabity of this
message for some time.

## handler

    handler

You should implement a handler.

# SEE ALSO

[Amazon::API::SQS](https://metacpan.org/pod/Amazon%3A%3AAPI%3A%3ASQS), [Proc::Daemon](https://metacpan.org/pod/Proc%3A%3ADaemon)

# AUTHOR

Rob Lauer - <rclauer@gmail.com>

# LICENSE

Copyright (c) 2025 TBC Development Group, LLC. All rights
reserved. This program is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.
