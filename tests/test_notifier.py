#
# Copyright (c) 2015 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import os.path
import sys
import json
from mock import patch, MagicMock

#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
#path = os.path.join(path, 'pynats')
#sys.path.insert(1, path)

from data_catalog.notifier import (CFNotifier)
from tests.base_test import DataCatalogTestCase

class CFNotifierTests(DataCatalogTestCase):

    @patch('pynats.Connection')
    def test_notify_correctMessgeSend(self, mock_connection):
        message = 'message'
        guid = 'guid'
        mock_con_val = mock_connection.return_value

        notifier = CFNotifier(self._config)
        notifier.notify(message, guid)

        self.assertTrue(mock_con_val.connect.called)
        self.assertTrue(mock_con_val.publish.called)

        subject = mock_con_val.publish.call_args[0][0]
        self.assertEquals(subject, self._config.services_url.nats_subject)

        publish_json = mock_con_val.publish.call_args[0][1]
        publish_data = json.loads(publish_json)
        self.assertEquals(message, publish_data['Message'])
        self.assertEquals(guid, publish_data['OrgGuid'])
        self.assertIn('Timestamp', publish_data.keys())



