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
import logging

from data_catalog.metadata_entry import IndexedMetadataEntry


class ElasticSearchQueryTranslator(object):

    def __init__(self):
        self._log = logging.getLogger(type(self).__name__)
        self._filter_translator = ElasticSearchFilterExtractor()
        self._base_query_creator = ElasticSearchBaseQueryCreator()

    def translate(self, data_catalog_query, org_uuid_list, dataset_filtering, is_admin):
        """
        Translates a Data Catalog query (string) to a string being an ElasticSearch query.
        match_all will be returned when the query is empty.
        Errors will be returned on invalid queries.
        :param data_catalog_query: A query string from Data Catalog
        :type data_catalog_query: str
        :param org_uuid_list: A list of org_uuids that dataset belongs to
        :type org_uuid_list: list[str]
        :param dataset_filtering: Describes if the data sets we want, should be private, public or both
                (takes values respectively: False, True, None)
        :type dataset_filtering: DataSetFiltering
        :returns A JSON string that is a valid ElasticSearch query
        :rtype str
        :raises ValueError
        """
        query_dict = self._get_query_dict(data_catalog_query)

        es_query_base = self._base_query_creator.create_base_query(query_dict)
        query_filters, post_filters = self._filter_translator.extract_filter(query_dict, org_uuid_list, dataset_filtering, is_admin)
        final_query = self._combine_query_and_filters(es_query_base, query_filters, post_filters)

        self._add_pagination(final_query, query_dict)
        return json.dumps(final_query)

    def _get_query_dict(self, data_catalog_query):
        """
        Translates a Data Catalog query from string to a dictionary.
        """
        if data_catalog_query:
            try:
                query_dict = json.loads(data_catalog_query)
            except ValueError:
                self._log_and_raise_invalid_query('Supplied query is not a JSON document.')
        else:
            query_dict = {}
        return query_dict

    @staticmethod
    def _combine_query_and_filters(base_es_query, query_filters, post_filters):
        """
        Combines translated base query, filters into one output query and aggregation for categories
        """
        return {
            'query': {
                'filtered': {
                    'filter': query_filters,
                    'query': base_es_query
                }
            },
            'post_filter': post_filters,
            'aggregations': {
                'categories': {
                    'terms': {
                        'field': 'category'
                    }
                },
                'formats': {
                    'terms': {
                        'field': 'format'
                    }
                }
            }
        }

    @staticmethod
    def _add_pagination(final_query, input_query_dict):
        """
        If input query contains pagination information ("from" and "size" fields) then they
        will be added to the output query.
        """
        from_field = 'from'
        size_field = 'size'
        if from_field in input_query_dict:
            final_query[from_field] = input_query_dict[from_field]
        if size_field in input_query_dict:
            final_query[size_field] = input_query_dict[size_field]

    def _log_and_raise_invalid_query(self, message):
        self._log.error(message)
        raise InvalidQueryError(message)


class ElasticSearchBaseQueryCreator(object):

    @staticmethod
    def create_base_query(query_dict):
        """
        Creates a base (text) query for the overall ElasticSearch query (which can contain both
        base query and filters).
        This query is created based on the "query" field from the Data Catalog query.
        A match_all query is returned when there's no text query.
        :param query_dict: A Data Catalog query in a form of dict (can be empty).
        :type query_dict: dict
        :returns A dictionary that represents a valid ElasticSearch query.
        :rtype dict
        """
        query_string = query_dict.get('query', None)
        if query_string:
            return {
                'bool': {
                    'should': [
                        {
                            'match': {
                                'title': {
                                    'query': query_string,
                                    'boost': 3,
                                    'fuzziness': 1
                                }
                            }
                        },
                        {
                            'match': {
                                'dataSample': {
                                    'query': query_string,
                                    'boost': 2
                                }
                            }
                        },
                        {
                            'match': {
                                'sourceUri': {
                                    'query': query_string,
                                }
                            }
                        }
                    ]
                }
            }
        else:
            return {'match_all': {}}


class ElasticSearchFilterExtractor(object):

    def __init__(self):
        self._log = logging.getLogger(type(self).__name__)

    def extract_filter(self, query_dict, org_uuid_list, dataset_filtering, is_admin):
        """
        Creates a filter for the ElasticSearch query based on the filter information
        from the Data Catalog query.
        None is returned when there are no filters.
        :param query_dict: A Data Catalog query in a form of dict (can be empty)
        :type query_dict: dict
        :param org_uuid_list: List of the organisations' UUIDs
        :type org_uuid_list: list[str]
        :returns Two types of filters; each as a dict {'and': [filter1, filter2, ...]}
        :rtype dict, dict
        """
        filters = query_dict.get('filters', [])
        query_filters = []
        post_filters = []
        or_filters = []

        if dataset_filtering is DataSetFiltering.PRIVATE_AND_PUBLIC:
            if not is_admin or org_uuid_list:
                filters.append({'orgUUID': org_uuid_list})
                filters.append({'isPublic': [True]})
        elif dataset_filtering is DataSetFiltering.ONLY_PRIVATE:
            if not is_admin or org_uuid_list:
                filters.append({'orgUUID': org_uuid_list})
            filters.append({'isPublic': [False]})
        else:
            filters.append({'isPublic': [True]})

        # filters should be in form NAME: [VALUE, VALUE, ...]
        for data_set_filter in filters:
            filter_type, filter_values = self._get_filter_properties(data_set_filter)
            es_filter = self._translate_filter(filter_type, filter_values)
            if not es_filter:
                continue
            if dataset_filtering is DataSetFiltering.PRIVATE_AND_PUBLIC:
                if filter_type in [IndexedMetadataEntry.ORG_UUID_FIELD,
                                   IndexedMetadataEntry.IS_PUBLIC_FIELD]:
                    # filters that are applied with 'or' parameter
                    or_filters.append(es_filter)
                elif filter_type in [IndexedMetadataEntry.CREATION_TIME_FIELD]:
                    # filters that are applied with the query (result are filtered)
                    query_filters.append(es_filter)
                else:
                    # filters that are applied AFTER the query (results are unfiltered)
                    post_filters.append(es_filter)
            else:
                if filter_type in [IndexedMetadataEntry.ORG_UUID_FIELD,
                                   IndexedMetadataEntry.CREATION_TIME_FIELD,
                                   IndexedMetadataEntry.IS_PUBLIC_FIELD]:
                    # filters that are applied with the query (result are filtered)
                    query_filters.append(es_filter)
                else:
                    # filters that are applied AFTER the query (results are unfiltered)
                    post_filters.append(es_filter)

        if not query_filters and or_filters:
            query_filters_dict = {'or': or_filters}
        elif or_filters and query_filters:
            query_filters.append({'or': or_filters})
            query_filters_dict = {'and': query_filters}
        elif not or_filters and query_filters:
            query_filters_dict = {'and': query_filters}
        else:
            query_filters_dict = {}

        if post_filters:
            return query_filters_dict, {'and': post_filters}
        else:
            return query_filters_dict, {}

    def _get_filter_properties(self, query_filter):
        """
        Gets a tuple: (filter_type, filter_values_list).
        Filter should be a dict in form: {FILTER_TYPE: FILTER_VALUES_LIST}
        """
        if not isinstance(query_filter, dict):
            self._log_and_raise_invalid_query(
                "A filter is not a dictionary: {}".format(query_filter))
        if not query_filter:
            self._log_and_raise_invalid_query("Filter dictionary can't be empty.")

        filter_type, filter_values = query_filter.items()[0]

        if filter_type not in IndexedMetadataEntry.resource_fields:
            self._log_and_raise_invalid_query(
                "Can't filter over field {}, because it isn't in the mapping.".format(filter_type))
        if not filter_values:
            self._log_and_raise_invalid_query("Filter doesn't contain any values")
        return filter_type, filter_values

    def _translate_filter(self, filter_type, filter_values):
        """
        Translates a filter of the given type with the given values list
        to an ElasticSearch filter.
        """

        def create_normal_filter(values):
            values = [str(value).lower() for value in values]
            if len(values) == 1:
                return {'term': {filter_type: values[0]}}
            else:
                return {'terms': {filter_type: values}}

        def create_time_filter(values):
            time_range = {}
            if len(values) != 2:
                self._log_and_raise_invalid_query('There should be exactly two time range values.')

            if values[0] != -1:
                time_range['from'] = values[0]
            if values[1] != -1:
                time_range['to'] = values[1]
            return {
                'range': {
                    IndexedMetadataEntry.CREATION_TIME_FIELD: time_range
                }
            }

        if not filter_values:
            return None
        elif not isinstance(filter_values, list):
            self._log_and_raise_invalid_query("Filter values aren't a list.")

        if filter_type != IndexedMetadataEntry.CREATION_TIME_FIELD:
            return create_normal_filter(filter_values)
        else:
            return create_time_filter(filter_values)

    def _log_and_raise_invalid_query(self, message):
        self._log.error(message)
        raise InvalidQueryError(message)


class InvalidQueryError(Exception):
    pass


class DataSetFiltering(object):
    PRIVATE_AND_PUBLIC = None
    ONLY_PUBLIC = True
    ONLY_PRIVATE = False
