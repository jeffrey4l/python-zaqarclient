# Copyright 2014 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
import json
import logging

from cliff import command
from cliff import lister
from cliff import show

from openstackclient.common import utils
from zaqarclient.transport import errors


def json_type(value):
    try:
        return json.loads(value)
    except ValueError:
        msg = "invalid json value: '%s'" % value
        raise argparse.ArgumentTypeError(msg)


class QueueMixin(object):
    """Queue Mixin."""

    def get_client(self):
        return self.app.client_manager.messaging

    def get_queue(self, queue_name, auto_create=True):
        queue = self.get_client().queue(queue_name,
                                        auto_create=auto_create)
        if not auto_create and not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)
        return queue

    def get_message_by_id(self, queue_name, message_id):
        queue = self.get_queue(queue_name, auto_create=False)
        try:
            return queue.message(message_id)
        except errors.ResourceNotFound:
            raise RuntimeError("Message(%s) does not found." % message_id)

    def get_messages_by_ids(self, queue_name, message_ids):
        queue = self.get_queue(queue_name, auto_create=False)
        return queue.messages(*message_ids)

    def get_claim_by_id(self, queue_name, claim_id):
        queue = self.get_queue(queue_name, auto_create=False)
        try:
            claim = queue.claim(id=claim_id)
            # NOTE(jeffrey4l): Trigger the _get method to fetch
            # the real object.
            claim._get()
        except errors.ResourceNotFound:
            raise RuntimeError("Claim(%s) does not exist." % claim_id)


class CreateQueue(show.ShowOne, QueueMixin):
    """Create a queue."""

    log = logging.getLogger(__name__ + ".CreateQueue")

    def get_parser(self, prog_name):
        parser = super(CreateQueue, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        data = self.get_queue(parsed_args.queue_name)

        columns = ('Name',)
        return columns, utils.get_item_properties(data, columns)


class DeleteQueue(command.Command, QueueMixin):
    """Delete a queue."""

    log = logging.getLogger(__name__ + ".DeleteQueue")

    def get_parser(self, prog_name):
        parser = super(DeleteQueue, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name)
        queue.delete()


class ListQueues(lister.Lister, QueueMixin):
    """List available queues."""

    log = logging.getLogger(__name__ + ".ListQueues")

    def get_parser(self, prog_name):
        parser = super(ListQueues, self).get_parser(prog_name)
        parser.add_argument(
            "--marker",
            metavar="<queue_id>",
            help="Queue's paging marker")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            help="Page size limit")

        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        kwargs = {}
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit

        data = self.get_client().queues(**kwargs)
        columns = ("Name", )
        return (columns,
                (utils.get_item_properties(s, columns) for s in data))


class CheckQueueExistence(show.ShowOne, QueueMixin):
    """Check queue existence."""

    log = logging.getLogger(__name__ + ".CheckQueueExistence")

    def get_parser(self, prog_name):
        parser = super(CheckQueueExistence, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_client().queue(parsed_args.queue_name,
                                        auto_create=False)

        columns = ('Exists',)
        data = dict(exists=queue.exists())
        return columns, utils.get_dict_properties(data, columns)


class SetQueueMetadata(command.Command, QueueMixin):
    """Set queue metadata."""

    log = logging.getLogger(__name__ + ".SetQueueMetadata")

    def get_parser(self, prog_name):
        parser = super(SetQueueMetadata, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        parser.add_argument(
            "queue_metadata",
            metavar="<queue_metadata>",
            type=json_type,
            help="Queue metadata")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)
        queue.metadata(new_meta=parsed_args.queue_metadata)


class GetQueueMetadata(show.ShowOne, QueueMixin):
    """Get queue metadata."""

    log = logging.getLogger(__name__ + ".GetQueueMetadata")

    def get_parser(self, prog_name):
        parser = super(GetQueueMetadata, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)

        columns = ("Metadata",)
        data = dict(metadata=queue.metadata())
        return columns, utils.get_dict_properties(data, columns)


class GetQueueStats(show.ShowOne, QueueMixin):
    """Get queue stats."""

    log = logging.getLogger(__name__ + ".GetQueueStats")

    def get_parser(self, prog_name):
        parser = super(GetQueueStats, self).get_parser(prog_name)
        parser.add_argument(
            "queue_name",
            metavar="<queue_name>",
            help="Name of the queue")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)
        columns = ("Stats",)
        data = dict(stats=queue.stats)
        return columns, utils.get_dict_properties(data, columns)


class PostMessage(show.ShowOne, QueueMixin):
    """Create a message in the queue."""

    log = logging.getLogger(__name__ + ".PostMessage")

    def get_parser(self, prog_name):
        parser = super(PostMessage, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "--ttl",
            metavar="<ttl>",
            required=True,
            type=int,
            help="How long the server should keep the message")
        parser.add_argument(
            "--body",
            metavar="<body>",
            required=True,
            help="")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)

        message = {
            "ttl": parsed_args.ttl,
            "body": parsed_args.body
        }

        message_ref = queue.post(message)

        columns = ("Resources",)
        return columns, utils.get_dict_properties(message_ref, columns)


class ListMessages(lister.Lister, QueueMixin):
    """List messages in the queue."""

    log = logging.getLogger(__name__ + ".ListMessages")

    def get_parser(self, prog_name):
        parser = super(ListMessages, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "--marker",
            metavar="<message_id>",
            help="Message's paging marker")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            type=int,
            help="Page size limit")
        parser.add_argument(
            "--echo",
            action="store_true",
            default=False,
            help="Whether or not return the client's own messages")
        parser.add_argument(
            "--include-claimed",
            action="store_true",
            default=False,
            help="Whether or not return claimed messages")

        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        kwargs = {}
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit
        if parsed_args.echo:
            kwargs["echo"] = True
        if parsed_args.include_claimed:
            kwargs["include_claimed"] = True

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)

        messages = queue.messages(**kwargs)
        columns = ("ID", "TTL", "Age")

        return (columns,
                (utils.get_item_properties(m, columns) for m in messages))


class GetMessagesById(lister.Lister, QueueMixin):
    """Get Messages by id."""

    log = logging.getLogger(__name__ + ".GetMessagesSetById")

    def get_parser(self, prog_name):
        parser = super(GetMessagesById, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "message_ids",
            metavar="<message_ids>",
            nargs="+",
            help="Id set of the message")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        messages = self.get_messages_by_ids(parsed_args.queue_name,
                                            parsed_args.message_ids)
        columns = ("ID", "TTL", "Age", "Body", "Href")
        return (columns,
                (utils.get_item_properties(message, columns)
                 for message in messages))


class DeleteMessagesById(command.Command, QueueMixin):
    """Delete message by id."""

    log = logging.getLogger(__name__ + ".DeleteMessageById")

    def get_parser(self, prog_name):
        parser = super(DeleteMessagesById, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "message_id",
            metavar="<message_id>",
            nargs="+",
            help="Id of the message")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)

        try:
            queue.delete_messages(*parsed_args.message_id)
        except errors.ResourceNotFound:
            raise RuntimeError("Message(%s) does not found." %
                               parsed_args.message_id)


class CreateClaim(show.ShowOne, QueueMixin):
    """Claim messages."""

    log = logging.getLogger(__name__ + ".CreateClaim")

    def get_parser(self, prog_name):
        parser = super(CreateClaim, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "--limit",
            metavar="<limit>",
            type=int,
            help="Page size limit.")
        parser.add_argument(
            "--ttl",
            metavar="<ttl>",
            type=int,
            required=True,
            help="How long the server should wait before releasing the claim")
        parser.add_argument(
            "--grace",
            metavar="<grace>",
            type=int,
            help="Message grace period in seconds")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        queue = self.get_queue(parsed_args.queue_name,
                               auto_create=False)
        claim = queue.claim(ttl=parsed_args.ttl,
                            grace=parsed_args.grace,
                            limit=parsed_args.limit)
        columns = ("id", 'ttl', 'age')
        if claim.id is None:
            # NOTE(jeffrey4l): When the id is None, it means there is no
            # unclaimed messages are available. So just return empty
            return (columns, ())
        else:
            return columns, utils.get_item_properties(claim, columns)


class QueryClaim(show.ShowOne, QueueMixin):
    """Query claim."""

    log = logging.getLogger(__name__ + ".QueryClaim")

    def get_parser(self, prog_name):
        parser = super(QueryClaim, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "id",
            metavar="<id>",
            help="Id of the claim")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        claim = self.get_claim_by_id(parsed_args.queue_name,
                                     parsed_args.id)
        columns = ("id", 'ttl', 'age', "messages")
        return columns, utils.get_item_properties(claim, columns)


class UpdateClaim(command.Command, QueueMixin):
    """Update claim."""

    log = logging.getLogger(__name__ + ".UpdateClaim")

    def get_parser(self, prog_name):
        parser = super(UpdateClaim, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "id",
            metavar="<id>",
            help="Id of the claim")
        parser.add_argument(
            "--ttl",
            metavar="<ttl>",
            type=int,
            required=True,
            help=("Number of seconds the server will be wait before"
                  " releasing the claim"))
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        claim = self.get_claim_by_id(parsed_args.queue_name,
                                     parsed_args.id)
        claim.update(ttl=parsed_args.ttl)


class ReleaseClaim(command.Command, QueueMixin):
    """Release claim."""

    log = logging.getLogger(__name__ + ".ReleaseClaim")

    def get_parser(self, prog_name):
        parser = super(ReleaseClaim, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "id",
            metavar="<id>",
            help="Id of the claim")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        claim = self.get_claim_by_id(parsed_args.queue_name,
                                     parsed_args.id)
        claim.delete()


class CreatePool(show.ShowOne, QueueMixin):
    """Create pool."""

    log =  logging.getLogger(__name__ + ".CreatePool")

    def get_parser(self, prog_name):
        parser = super(CreatePool, self).get_parser(prog_name)
        parser.add_argument(
            "pool_name",
            metavar="<pool_name>",
            help="The name of the storage pool entry")
        parser.add_argument(
            "--weight",
            metavar="<weight>",
            type=int,
            help="")
        parser.add_argument(
            "--uri",
            metavar="<uri>",
            help="")
        parser.add_argument(
            "--options",
            metavar="<options>",
            help="")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        kwargs = {'auto_create': True}
        if parsed_args.weight is not None:
            kwargs['weight'] = parsed_args.weight
        if parsed_args.uri is not None:
            kwargs['uri'] = parsed_args.uri
        if parsed_args.options is not None:
            kwargs['options'] = parsed_args.options

        pool = self.get_client().pool(parsed_args.pool_name,
                                      **kwargs)
        columns = ('name', 'weight', 'uri', 'options')
        return columns, utils.get_item_properties(pool, columns)


class GetPoolInfo(show.ShowOne, QueueMixin):
    def take_action(self, parsed_args):
        pass


class DeletePool(command.Command, QueueMixin):
    def take_action(self, parsed_args):
        pass


class ListPools(lister.Lister, QueueMixin):

    def get_parser(self, prog_name):
        parser = super(ListPools, self).get_parser(prog_name)
        return parser

    def take_action(self, parsed_args):
        pass