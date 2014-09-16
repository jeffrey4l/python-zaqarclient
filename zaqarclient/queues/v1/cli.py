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

import json
import logging

from cliff import command
from cliff import lister
from cliff import show

from openstackclient.common import utils
from zaqarclient.transport import errors


class CreateQueue(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        data = client.queue(queue_name)

        columns = ('Name',)
        properties = ("_Name",)
        return columns, utils.get_item_properties(data, properties)


class DeleteQueue(command.Command):
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
        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name

        client.queue(queue_name).delete()


class ListQueues(lister.Lister):
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

        client = self.app.client_manager.queuing

        kwargs = {}
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit

        data = client.queues(**kwargs)
        columns = ("Name", )
        properties = ("_Name",)
        return (columns,
                (utils.get_item_properties(s, properties) for s in data))


class CheckQueueExistence(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        columns = ('Exists',)
        data = dict(exists=queue.exists())
        return columns, utils.get_dict_properties(data, columns)


class SetQueueMetadata(command.Command):
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
            help="Queue metadata")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue_metadata = parsed_args.queue_metadata
        queue_exists = client.queue(queue_name, auto_create=False).exists()

        if not queue_exists:
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        try:
            valid_metadata = json.loads(queue_metadata)
        except ValueError:
            raise RuntimeError("Queue metadata(%s) is not a valid json." %
                               queue_metadata)

        client.queue(queue_name, auto_create=False).\
            metadata(new_meta=valid_metadata)


class GetQueueMetadata(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        columns = ("Metadata",)
        data = dict(metadata=queue.metadata())
        return columns, utils.get_dict_properties(data, columns)


class GetQueueStats(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        if not queue.exists():
            raise RuntimeError('Queue(%s) does not exist.' % queue_name)

        columns = ("Stats",)
        data = dict(stats=queue.stats)
        return columns, utils.get_dict_properties(data, columns)


class PostMessage(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        message = {
            "ttl": parsed_args.ttl,
            "body": parsed_args.body
        }

        message_ref = queue.post(message)

        columns = ("Resources",)
        return columns, utils.get_dict_properties(message_ref, columns)


class ListMessages(lister.Lister):
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

        client = self.app.client_manager.queuing

        kwargs = {}
        if parsed_args.marker is not None:
            kwargs["marker"] = parsed_args.marker
        if parsed_args.limit is not None:
            kwargs["limit"] = parsed_args.limit
        if parsed_args.echo:
            kwargs["echo"] = True
        if parsed_args.include_claimed:
            kwargs["include_claimed"] = True

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        messages = queue.messages(**kwargs)
        columns = ("ID", "TTL", "Age")
        properties = ("_id", "ttl", "age")

        return (columns,
                (utils.get_item_properties(m, properties) for m in messages))


class GetMessageById(show.ShowOne):
    """Get Message by id."""

    log = logging.getLogger(__name__ + ".GetMessageById")

    def get_parser(self, prog_name):
        parser = super(GetMessageById, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "message_id",
            metavar="<message_id>",
            help="Id of the message")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        try:
            message = queue.message(parsed_args.message_id)
            columns = ("id", "ttl", "age", "body", "href")
            properties = ("_id", "ttl", "age", "body", "href")
            return columns, utils.get_item_properties(message, properties)
        except errors.ResourceNotFound:
            raise RuntimeError("Message(%s) does not found." %
                               parsed_args.message_id)


class GetMessagesSetById(lister.Lister):
    """Get Messages set by id."""

    log = logging.getLogger(__name__ + ".GetMessagesSetById")

    def get_parser(self, prog_name):
        parser = super(GetMessagesSetById, self).get_parser(prog_name)
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name, auto_create=False)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        try:
            messages = queue.messages(*parsed_args.message_ids)
            columns = ("ID", "TTL", "Age", "Body", "Href")
            properties = ("_id", "ttl", "age", "body", "href")
            return (columns,
                    (utils.get_item_properties(message, properties)
                     for message in messages))
        except errors.ResourceNotFound:
            raise RuntimeError("Message(%s) does not found." %
                               parsed_args.message_id)


class DeleteMessageById(command.Command):
    """Delete message by id."""

    log = logging.getLogger(__name__ + ".DeleteMessageById")

    def get_parser(self, prog_name):
        parser = super(DeleteMessageById, self).get_parser(prog_name)
        parser.add_argument(
            "--queue_name",
            metavar="<queue_name>",
            required=True,
            help="Name of the queue")
        parser.add_argument(
            "message_id",
            metavar="<message_id>",
            help="Id of the message")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        try:
            message = queue.message(parsed_args.message_id)
            message.delete()
        except errors.ResourceNotFound:
            raise RuntimeError("Message(%s) does not found." %
                               parsed_args.message_id)


class DeleteMessagesSetById(command.Command):
    """Delete messages set by id."""

    log = logging.getLogger(__name__ + ".DeleteMessagesSetById")

    def get_parser(self, prog_name):
        parser = super(DeleteMessagesSetById, self).get_parser(prog_name)
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        try:
            messages = queue.messages(*parsed_args.message_id)
            if messages:
                for message in messages:
                    message.delete()
        except errors.ResourceNotFound:
            raise RuntimeError("Message(%s) does not found." %
                               parsed_args.message_id)


class CreateClaim(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        claim = queue.claim(ttl=parsed_args.ttl,
                            grace=parsed_args.grace,
                            limit=parsed_args.limit)

        columns = ("id", 'ttl', 'age', "grace")
        return columns, utils.get_item_properties(claim, columns)


class QueryClaim(show.ShowOne):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        claim = queue.claim(id=parsed_args.id)
        columns = ("id", 'ttl', 'age', "messages")
        return columns, utils.get_item_properties(claim, columns)


class UpdateClaim(command.Command):
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
            help="How long the server should wait before releasing the claim")
        return parser

    def take_action(self, parsed_args):
        self.log.debug("take_action(%s)" % parsed_args)

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        claim = queue.claim(id=parsed_args.id)
        # TODO(jeffrey4l): Could the grace field be updated?
        claim.update(ttl=parsed_args.ttl)


class ReleaseClaim(command.Command):
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

        client = self.app.client_manager.queuing

        queue_name = parsed_args.queue_name
        queue = client.queue(queue_name)

        if not queue.exists():
            raise RuntimeError("Queue(%s) does not exist." % queue_name)

        claim = queue.claim(id=parsed_args.id)
        claim.delete()
