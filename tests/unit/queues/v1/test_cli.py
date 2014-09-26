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

from zaqarclient.queues.v1 import cli
from zaqarclient.tests.queues import cli as cli_test


class QueueTestCase(cli_test.CliTestCase):

    def test_get_parser(self):
        self.messaging.queue.return_value = cli_test.FakeResource(cli_test.QUEUE)

        create_queue = cli.CreateQueue(self.app, None)
        parsed_args = self.check_parser(create_queue,
                                        ["test"],
                                        {"queue_name": "test"})

        columns, data = create_queue.take_action(parsed_args)

        colexpect = ('Name',)
        dataexp = ('test_queue', )

        self.assertEqual(colexpect, columns)
        self.assertEqual(dataexp, data)

    def test_delete(self):
        delete_queue = cli.DeleteQueue(self.app, None)

        parsed_args = self.check_parser(delete_queue,
                                        ['test_queue'],
                                        {"queue_name": "test_queue"})
        delete_queue.take_action(parsed_args)

        self.assertTrue(self.messaging.queue.called_with('test_queue'))
        self.assertTrue(self.messaging.queue().delete.called)

    def test_list_no_args(self):
        list_queue = cli.ListQueues(self.app, None)

        parsed_args = self.check_parser(list_queue, [], {})
        self.assertIsNone(parsed_args.marker)
        self.assertIsNone(parsed_args.limit)

    def test_list_with_args(self):
        self.messaging.queues.return_value = [cli_test.FakeResource(cli_test.QUEUE)]
        list_queue = cli.ListQueues(self.app, None)

        cmd = ["--marker", "marker1", "--limit", "10"]
        parsed_args = self.check_parser(list_queue,
                                        cmd,
                                        {"marker": "marker1",
                                         "limit": "10"})
        columns, data = list_queue.take_action(parsed_args)

        expect_col = ("Name",)
        expect_data = [("test_queue",)]

        self.assertEqual(expect_col, columns)
        self.assertEqual(expect_data, list(data))
