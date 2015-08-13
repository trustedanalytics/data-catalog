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
from data_catalog import configuration_const

VCAP_APPLICATION = 'VCAP_APPLICATION'
VCAP_SERVICES = 'VCAP_SERVICES'
VCAP_APP_PORT = 'VCAP_APP_PORT'
LOG_LEVEL = 'LOG_LEVEL'

class DCConfig(object):

    """
    Contains a configuration of the Data Catalog.
    No fields should  be written to be the user.
    """

    def __init__(self):
        """
        Reads the configuration from environment variables set by Cloud Foundry (CF)
        or by the developer when running locally.
        Normally, CF sets the full configuration in environment variables,
        e.g. VCAP_SERVICES (which is most important for us).
        When working on a local machine, VCAP_SERVICES also needs to be set and contain some values.
        Without them, the app couldn't work.
        Most of configuration values have some defaults that will be set when they aren't configured
        in the environment, but some of them, like the address of the authorization server can't
        have a default. They need to be set manually by the developer to enable local work.
        """
        self._fail_if_no_configuration()

        self.app_base_path = '/rest/datasets'

        # if values aren't found we set defaults that are meant for local operations
        self.app_port = int(os.getenv(VCAP_APP_PORT, '5000'))
        self.log_level = os.getenv(LOG_LEVEL, 'DEBUG')

        services_config = json.loads(os.environ[VCAP_SERVICES])
        self.elastic = ElasticConfig(services_config)
        self.services_url = ServiceUrlsConfig(services_config)

    @staticmethod
    def _fail_if_no_configuration():
        """
        Checks the environment variables to decide if the app can run.
        :rtype: None
        :raises NoConfigEnvError: When VCAP_SERVICES is not present and the app can't run.
        """
        if not VCAP_SERVICES in os.environ:
            raise NoConfigEnvError(
                'VCAP_SERVICES environment variable needs to be set to run Data Catalog.')


class NoConfigEnvError(Exception):
    pass


class ElasticConfig(object):

    def __init__(self, services_config):
        self.elastic_index = 'trustedanalytics-meta'
        self.elastic_metadata_type = 'dataset'
        self.elastic_categories_type = 'categories'

        self.metadata_index_setup = {
            'settings': {
                'index': configuration_const.METADATA_SETTINGS
            },
            'mappings': {
                self.elastic_metadata_type: configuration_const.METADATA_MAPPING
            }
        }

        try:
            elastic_credentials = services_config['elasticsearch13'][0]['credentials']
            self.elastic_hostname = elastic_credentials['hostname']
            self.elastic_port = int(elastic_credentials['ports']['9200/tcp'])
        except KeyError:
            self.elastic_hostname = 'localhost'
            self.elastic_port = 9200


class ServiceUrlsConfig(object):

    def __init__(self, services_config):
        self.uaa_token_uri = self._get_uaa_token_uri(services_config)
        self.downloader_url_pattern = self._configure_downloader_services(services_config)
        self.dataset_publisher_url = self._cfg_data_publisher_services(services_config)
        self.user_management_uri = self._configure_user_management(services_config)

    @staticmethod
    def _get_credential(services):
        return ServiceUrlsConfig._find_by_name_in_service(services, 'sso')['credentials']

    def _get_uaa_token_uri(self, services):
        """
        :param services: Json loaded from VCAP_SERVICES environment variable
        :type services: dict
        :return: Address from which UAA's public key can be obtained
        :rtype: str
        :raises NoConfigEnvError: When VCAP_SERVICES doesn't contain 'tokenKey' setting.
        """
        try:
            return self._get_credential(services)['tokenKey']
        except KeyError:
            raise NoConfigEnvError('No SSO/tokenKey parameter in VCAP_SERVICES.')

    def _configure_downloader_services(self, services_config):
        #TODO this should be simplified after removing the first way of configuring downloader
        downloader_url_pattern_suffix = '/rest/filestore/{}/'

        try:
            DOWNLOADER = 'downloader'
            if DOWNLOADER in services_config:
                downloader_config = services_config[DOWNLOADER][0]
            else:
                downloader_config = self._find_by_name_in_service(
                    services_config, DOWNLOADER)
            return downloader_config['credentials']['url'] + downloader_url_pattern_suffix
        except KeyError:
            return 'http://localhost:8090' + downloader_url_pattern_suffix

    def _cfg_data_publisher_services(self, services_config):
        dataset_publisher_url_suffix = '/rest/tables'

        try:
            dataset_publisher_host = self._find_by_name_in_service(services_config, "datacatalogexport")
            dataset_publisher_host = dataset_publisher_host['credentials']['host']
            return dataset_publisher_host + dataset_publisher_url_suffix
        except KeyError:
            return 'http://localhost:8091' + dataset_publisher_url_suffix

    def _configure_user_management(self, services_config):
        user_management_url_suffix = '/rest/orgs/permissions'

        try:
            user_management_host = self._find_by_name_in_service(services_config, "user-management")
            user_management_host = user_management_host['credentials']['host']
            return user_management_host + user_management_url_suffix
        except KeyError:
            return 'http://localhost:9998' + user_management_url_suffix

    @staticmethod
    def _find_by_name_in_service(services, name):
        found = [x for x in services['user-provided'] if x['name'] == name]
        return found[0] if found else {}
