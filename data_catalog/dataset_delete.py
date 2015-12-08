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

import requests
from data_catalog.bases import DataCatalogModel


class DataSetRemover(DataCatalogModel):

    def delete(self, entry_id, token):
        """
        Deletes data set information from ElasticSearch and requests deleting from other services.
        :param entry_id: elastic search id
        :param token: authorization token
        :raises NotFoundError: entry not found in Elastic Search
        :raises ConnectionError: problem with connecting to Elastic Search
        """
        elastic_data = self._get_entry(entry_id)
        metadata = elastic_data["_source"]
        target_uri = elastic_data["_source"]["targetUri"]

        self._delete_entry(entry_id)

        return {
            "deleted_from_downloader": self._delete_from_downloader(target_uri, token),
            "deleted_from_publisher": self._delete_from_dataset_publisher(metadata, token)
        }

    def delete_public_table_from_dataset_publisher(self, entry_id, token):
        elastic_data = self._get_entry(entry_id)
        metadata = elastic_data["_source"]
        if metadata["isPublic"]:
            delete_url = self._config.services_url.dataset_publisher_url
            params = {"scope": "public"}
            return self._external_delete('Dataset Publisher', token, delete_url, metadata, params)

    def _delete_from_downloader(self, target_uri, token):
        delete_url = self._create_downloader_delete_url(target_uri)
        return self._external_delete('Downloader', token, delete_url)

    def _delete_from_dataset_publisher(self, metadata, token):
        delete_url = self._config.services_url.dataset_publisher_url
        return self._external_delete('Dataset Publisher', token, delete_url, metadata)

    def _external_delete(self, service_name, token, url, data=None, parameters=None):
        """
        Deletes a data set from an external service.
        :param str service_name: Service name. Only used in logging.
        :param str token: Security token that will be sent with the request.
        :param str url: URL to which to send DELETE message.
        :param dict data: Data to send in the DELETE request.
        :param dict parameters: parameters to send in the DELETE request
        :return: True if delete was successful, false otherwise
        :rtype: bool
        """
        self._log.info('Sending delete request to: %s', url)
        response = requests.delete(url, headers={'Authorization': token}, json=data, params=parameters)
        if response.status_code == 200:
            return True
        else:
            self._log.warning('Failed to delete data set from %s.', service_name)
            return False

    def _create_downloader_delete_url(self, target_uri):
        database_id = self._get_database_id_from_uri(target_uri)
        return self._config.services_url.downloader_url_pattern.format(database_id)

    @staticmethod
    def _get_database_id_from_uri(target_uri):
        target_uris = target_uri.split("/")
        database_id = target_uris[-2]
        return database_id
