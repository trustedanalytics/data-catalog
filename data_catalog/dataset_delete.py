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
        Deletes data set information from elastic search and
         file from database (downloader service responsible for handling file)
        :param entry_id: elastic search id
        :param token: authorization token
        :raises NotFoundError: entry not found in Elastic Search
        :raises ConnectionError: problem with connecting to Elastic Search
        :raises DataSetNotFound: entry not found in downloader service
        :raises DataSourceServiceError: problem with downloader service
        :raises DatasetPublisherServiceError: problem with dataset-publisher service
        """
        elastic_data = self._get_entry(entry_id)
        metadata = elastic_data["_source"]
        target_uri = elastic_data["_source"]["targetUri"]

        self._delete_entry(entry_id)

        try:
            self._delete_from_service(target_uri, token)
            downloader_status = True
        except Exception:
            self._log.exception("Cannot delete from store (downloader)")
            downloader_status = False

        try:
            self._delete_from_dataset_publisher(metadata, token)
            publisher_status = True
        except Exception:
            self._log.exception("Cannot delete from store (dataset-publiser)")
            publisher_status = False

        return {
            "deleted_from_downloader": downloader_status,
            "deleted_from_publisher": publisher_status
        }

    def _delete_from_service(self, target_uri, token):
        delete_url = self._create_delete_url(target_uri)
        self._log.info("Sending delete request send to: {}".format(delete_url))
        response = requests.delete(delete_url, headers={'Authorization': token})
        self._handle_downloader_status_code(response.status_code)

    def _delete_from_dataset_publisher(self, metadata, token):
        delete_url = self._config.services_url.dataset_publisher_url
        self._log.info("Sending delete request to: {}".format(delete_url))
        response = requests.delete(delete_url, headers={'Authorization': token}, json=metadata)
        self._handle_publisher_status_code(response.status_code)

    def _create_delete_url(self, target_uri):
        database_id = self._get_database_id_from_uri(target_uri)
        return self._config.services_url.downloader_url_pattern.format(database_id)

    @staticmethod
    def _get_database_id_from_uri(target_uri):
        target_uris = target_uri.split("/")
        database_id = target_uris[-2]
        return database_id

    @staticmethod
    def _handle_downloader_status_code(status_code):
        if status_code == 200:
            return
        elif status_code == 404:
            raise NotFoundInExternalService()
        elif status_code == 500:
            raise DataSourceServiceError()

        raise DataSourceServiceError("Unknown error in deleting dataset on HDFS."
                                     "Status code: {}".format(status_code))

    def _handle_publisher_status_code(self, status_code):
        self._log.debug("publisher service response: {}".format(status_code))
        if status_code != 200:
            raise DatasetPublisherServiceError("Error when deleting hive table."
                                               "Status code: {}".format(status_code))


class NotFoundInExternalService(Exception):
    pass


class DataSourceServiceError(Exception):
    pass


class DatasetPublisherServiceError(Exception):
    pass
