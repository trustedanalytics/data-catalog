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

import flask

from elasticsearch.exceptions import RequestError, ConnectionError
from flask_restful import abort

from data_catalog.bases import DataCatalogModel, DataCatalogResource
from data_catalog.query_translation import ElasticSearchQueryTranslator, \
    InvalidQueryError, DataSetFiltering


class DataSetSearchResource(DataCatalogResource):

    """
    Enables searching for metadata describing data sets.
    """

    def __init__(self):
        super(DataSetSearchResource, self).__init__()
        self._search = DataSetSearch()

    def get(self):
        """
        Do a search for data sets.
        Query should be in this format:
        {
            "query": SEARCH_TEXT,
            "filters":[
                {FILTERED_FIELD_NAME: [FIELD_VALUE_1, FIELD_VALUE_1]}
            ],
            "from": FROM_HIT_NUMBER,
            "size": NUMBER_OF_HITS
        }

        All query fields are optional.
        When filtering by time ranges, you must supply exactly two filter field values.
        -1 can be used as infinity.

        "from" and "size" are used for pagination of search queries.
        If we get 20 hits for a query, we can set "from" and "size" to 10
        to get the second half of hits.

        Filter examples:
        {"creationTime": [-1, "2015-02-24T14:56"]} <- all until 2015-02-24T14:56
        {"format": ["csv", "json"]} <- all CSV and JSON data sets

        Field 'orgs' should be in a form of a list of org uuids separated with a coma
        example: orguuid-01,oruuid-02

        Fields 'onlyPublic' and 'onlyPrivate' should have boolean value (true or false).
        In addition to a query, they allow to choose only private data sets or only public ones.
        They are mutually exclusive!

        """
        args = flask.request.args
        query_string = args.get('query')
        is_admin = flask.g.is_admin
        org_uuid_list = flask.g.get('org_uuid_list')
        params = self._search.get_params_from_request_args(args)
        try:
            return self._search.search(
                query_string, org_uuid_list,
                params['dataset_filtering'],
                is_admin)
        except InvalidQueryError:
            abort(400, message=DataSetSearch.INVALID_QUERY_ERROR_MESSAGE)
        except IndexConnectionError:
            abort(500, message=DataSetSearch.NO_CONNECTION_ERROR_MESSAGE)


class IndexConnectionError(Exception):
    pass


class DataSetSearch(DataCatalogModel):

    """
    Responsible for searching the ElasticSearch index for data sets
    (the metadata describing them).
    """

    SEARCH_ERROR_MESSAGE = 'Searching in the index failed'
    INVALID_QUERY_ERROR_MESSAGE = SEARCH_ERROR_MESSAGE + ': invalid query.'
    NO_CONNECTION_ERROR_MESSAGE = SEARCH_ERROR_MESSAGE + ': failed to connect to ElasticSearch.'

    def __init__(self):
        super(DataSetSearch, self).__init__()
        self._translator = ElasticSearchQueryTranslator()

    def search(self, query, org_uuid_list, dataset_filtering, is_admin):
        query_string = self._translator.translate(query, org_uuid_list, dataset_filtering, is_admin)
        try:
            elastic_search_results = self._elastic_search.search(
                index=self._config.elastic.elastic_index,
                doc_type=self._config.elastic.elastic_metadata_type,
                body=query_string
            )
            return self._extract_metadata(elastic_search_results)
        except RequestError:
            self._log.exception(self.INVALID_QUERY_ERROR_MESSAGE)
            raise InvalidQueryError(self.INVALID_QUERY_ERROR_MESSAGE)
        except ConnectionError:
            self._log.exception(self.NO_CONNECTION_ERROR_MESSAGE)
            raise IndexConnectionError(self.NO_CONNECTION_ERROR_MESSAGE)

    @staticmethod
    def _extract_metadata(es_query_result):
        hits = es_query_result['hits']
        category_aggregations = es_query_result['aggregations']['categories']['buckets']
        format_aggregations = es_query_result['aggregations']['formats']['buckets']
        entries = []
        for entry in hits['hits']:
            entries.append(entry['_source'])
            entries[-1]['id'] = entry['_id']
        categories = [cat['key'] for cat in category_aggregations]
        formats = [obj['key'] for obj in format_aggregations]
        return {'hits': entries,
                'total': hits['total'],
                'categories': categories,
                'formats': formats}

    @staticmethod
    def get_params_from_request_args(args):
        dataset_filtering = DataSetFiltering.PRIVATE_AND_PUBLIC
        if args.get('onlyPublic', default="", type=str).lower() == 'true':
            dataset_filtering = DataSetFiltering.ONLY_PUBLIC
        if args.get('onlyPrivate', default="", type=str).lower() == 'true':
            dataset_filtering = DataSetFiltering.ONLY_PRIVATE

        return {'dataset_filtering': dataset_filtering}
