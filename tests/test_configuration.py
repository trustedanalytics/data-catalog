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

import json
import os
import unittest

from data_catalog.configuration import DCConfig, NoConfigEnvError, VCAP_SERVICES
from base_test import fake_env, clean_fake_env


class ConfigTests(unittest.TestCase):
    def test_getConfig_noVcapServices_raiseError(self):
        with self.assertRaises(NoConfigEnvError):
            DCConfig()

    def test_getConfig_localConfig_returnFilledLocalConfig(self):
        os.environ[VCAP_SERVICES] = json.dumps({
            'user-provided': [
                {
                    'credentials': {'tokenKey': 'http://uaa.example.com/token_key'},
                    'tags': [],
                    'name': 'sso',
                    'label': 'user-provided'
                }
            ]
        })
        config = DCConfig()

        self.assertEqual(5000, config.app_port)
        self.assertEqual('DEBUG', config.log_level)

        self.assertEqual('localhost', config.elastic.elastic_hostname)
        self.assertEqual(9200, config.elastic.elastic_port)

        self.assertEqual(
            'http://localhost:8091/rest/tables',
            config.services_url.dataset_publisher_url)
        self.assertEqual(
            'http://localhost:9998/rest/orgs/permissions',
            config.services_url.user_management_uri)

        clean_fake_env()

    def test_getConfig_withCloudEnv_returnCloudConfig(self):
        with fake_env():
            config = DCConfig()
            self.assertEqual(5555, config.app_port)
            self.assertEqual('INFO', config.log_level)

            self.assertEqual('10.10.2.7', config.elastic.elastic_hostname)
            self.assertEqual(49237, config.elastic.elastic_port)

            self.assertEqual(
                'http://uaa.run.example.com/token_key',
                config.services_url.uaa_token_uri)
            self.assertEqual(
                'http://hive.apps.example.com/rest/tables',
                config.services_url.dataset_publisher_url)
            self.assertEqual(
                'http://user-management.apps.example.com/rest/orgs/permissions',
                config.services_url.user_management_uri)


    #TODO we should make downloader config not in user-provided services obsolete soon
    def test_getConfig_alternativeDownloaderSetup_downloaderUrlSet(self):
        def set_alternative_downloader_conf():
            vcap_services = json.loads(os.environ[VCAP_SERVICES])
            del vcap_services['downloader']
            os.environ[VCAP_SERVICES] = json.dumps(vcap_services)

        with fake_env():
            set_alternative_downloader_conf()
            config = DCConfig()
            self.assertEquals(
                'http://downloader-broker.apps.example.com/rest/filestore/{}/',
                config.services_url.downloader_url_pattern)


if __name__ == '__main__':
    unittest.main()
