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

import mock

from zaqarclient.tests import base


QUEUE_NAME = 'test_queue'
QUEUE = {
    'name': QUEUE_NAME,
    '_name': QUEUE_NAME
}


class FakeResource(object):
    def __init__(self, info):
        self._extra_info(info)

    def _extra_info(self, info):
        for attr, value in info.iteritems():
            setattr(self, attr, value)


class CliTestCase(base.TestBase):

    def setUp(self):
        super(CliTestCase, self).setUp()
        self.app = mock.Mock()
        self.messaging = self.app.client_manager.messaging

    def check_parser(self, cmd, args, varify_args):
        parser = cmd.get_parser('check_parser')
        parsed_args = parser.parse_args(args)
        for attr, value in varify_args.iteritems():
            self.assertIn(attr, parsed_args)
            self.assertEqual(value, getattr(parsed_args, attr, None))
        return parsed_args
