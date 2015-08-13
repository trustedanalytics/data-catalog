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

import os
import unittest
from contextlib import contextmanager
from mock import MagicMock

from data_catalog.configuration import (DCConfig, VCAP_APP_PORT, VCAP_SERVICES, VCAP_APPLICATION,
                                        LOG_LEVEL)


class DataCatalogTestCase(unittest.TestCase):
    def setUp(self):
        setup_fake_env()
        self._config = DCConfig()

        from data_catalog import app
        app.app.config['TESTING'] = True
        self.app = app.app
        self.client = self.app.test_client()

        self._disable_authentication()

    def tearDown(self):
        clean_fake_env()

    def _disable_authentication(self):
        mock = MagicMock()
        mock.mocked_function.side_effect = lambda: None
        self.client.application.before_request_funcs = {None: [mock.mocked_function]}
        self.mocked_authenticate = mock.mocked_function

def setup_fake_env():
    """
    Sets up a fake environment needed for the app to properly load configuration during tests.
    This fake environment looks like a major part of normal environment in Cloud Foundry.
    :rtype: None
    """
    os.environ[VCAP_SERVICES] = '''{
        "downloader": [
            {
                "credentials": {
                    "url": "http://downloader-broker.apps.example.com"
                },
                "label": "downloader",
                "name": "downloader",
                "plan": "shared",
                "syslog_drain_url": "",
                "tags": [
                    "simple",
                    "shared"
                ]
            }
        ],
        "elasticsearch13": [
            {
                "credentials": {
                    "hostname": "10.10.2.7",
                    "ports": {
                        "9200/tcp": "49237",
                        "9300/tcp": "49238"
                    }
                },
                "label": "elasticsearch13",
                "name": "data-catalog-mock-index",
                "plan": "free",
                "tags": [
                    "elasticsearch13",
                    "elasticsearch",
                    "search"
                ]
            }
        ],
        "user-provided": [
            {
                "credentials": {
                    "host": "http://hive.apps.example.com"
                },
                "label": "user-provided",
                "name": "datacatalogexport",
                "syslog_drain_url": "",
                "tags": []
            },
            {
                "credentials": {
                     "tokenKey": "http://uaa.run.example.com/token_key"
                },
                "label": "user-provided",
                "name": "sso",
                "syslog_drain_url": "",
                "tags": []
            },
            {
                "credentials": {
                    "url": "http://downloader-broker.apps.example.com"
                },
                "label": "user-provided",
                "name": "downloader",
                "syslog_drain_url": "",
                "tags": []
            },
            {
                "credentials": {
                    "host": "http://user-management.apps.example.com"
                },
                "label": "user-provided",
                "name": "user-management",
                "syslog_drain_url": "",
                "tags": []
            }
        ]
    }'''
    os.environ[VCAP_APP_PORT] = '5555'
    os.environ[LOG_LEVEL] = 'INFO'
    os.environ[VCAP_APPLICATION] = "this is set just so the config doesn't try to set local values"


def clean_fake_env():
    os.environ.pop(VCAP_SERVICES, None)
    os.environ.pop(VCAP_APP_PORT, None)
    os.environ.pop(LOG_LEVEL, None)
    os.environ.pop(VCAP_APPLICATION, None)


@contextmanager
def fake_env():
    """
    Sets a fake environment and gets rid of it after the function.
    """
    setup_fake_env()
    try:
        yield
    finally:
        clean_fake_env()
