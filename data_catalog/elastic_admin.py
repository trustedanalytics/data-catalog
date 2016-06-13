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

"""
Endpoint for administrative tasks on Data Catalog and its data.
"""

import flask
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError

from data_catalog.bases import DataCatalogResource
from data_catalog.metadata_entry import MetadataIndexingTransformer, InvalidEntryError


class ElasticSearchAdminResource(DataCatalogResource):
    """
    Contains REST endpoint for managing elastic search data
    """

    def __init__(self):
        super(ElasticSearchAdminResource, self).__init__()
        self._elastic_search = Elasticsearch(
            '{}:{}'.format(self._config.elastic.elastic_hostname,
                           self._config.elastic.elastic_port))
        self._parser = MetadataIndexingTransformer()

    def delete(self):
        """
        Delete elastic search index
        """
        self._log.info('Deleting the ElasticSearch index.')
        if not flask.g.is_admin:
            self._log.warn('Deleting index aborted, not enough privileges (admin required)')
            return None, 403
        # pylint: disable=unexpected-keyword-arg
        self._elastic_search.indices.delete(
            self._config.elastic.elastic_index,
            ignore=404)

    def put(self):
        """
        Add all data into elastic search. Data that are corrupted are ommited
        """
        self._log.info("Adding data to elastic search")
        if not flask.g.is_admin:
            self._log.warn('Inserting data aborted, not enough privileges (admin required)')
            return None, 403
        data = flask.request.get_json(force=True)

        try:
            for entry in data:
                try:
                    self._parser.transform(entry)
                    self._elastic_search.index(
                        index=self._config.elastic.elastic_index,
                        doc_type=self._config.elastic.elastic_metadata_type,
                        id=entry["id"],
                        body=entry
                    )
                except InvalidEntryError as ex:
                    self._log.exception(ex)
        except RequestError:
            self._log.exception("Malformed data")
            return None, 400
        except ConnectionError:
            self._log.exception("Failed connection to ElasticSearch")
            return None, 503
        self._log.info("Data added")
        return None, 200

