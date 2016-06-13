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

from datetime import datetime
from urlparse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError, NotFoundError
import flask
from flask import abort
from cerberus import Validator

from data_catalog.bases import DataCatalogResource
from data_catalog.bases import DataCatalogModel
from data_catalog.dataset_delete import DataSetRemover
from data_catalog.notifier import CFNotifier

# TODO dirty, but testable
CURRENT_TIME_FUNCTION = datetime.now

TITLE_FIELD = 'title'
CREATION_TIME_FIELD = 'creationTime'
CATEGORY_FIELD = 'category'
TARGET_URI_FIELD = 'targetUri'
IS_PUBLIC_FIELD = 'isPublic'
ORG_UUID_FIELD = 'orgUUID'

CERBERUS_SCHEMA = {
    CATEGORY_FIELD: {'required': True, 'type': 'string'},
    CREATION_TIME_FIELD: {'required': False, 'type': 'string'},
    'dataSample': {'required': True, 'type': 'string'},
    'format': {'required': True, 'type': 'string'},
    IS_PUBLIC_FIELD: {'required': True, 'type': 'boolean'},
    ORG_UUID_FIELD: {'required': True, 'type': 'string'},
    'recordCount': {'required': True, 'type': 'integer'},
    'size': {'required': True, 'type': 'integer'},
    'sourceUri': {'required': True, 'type': 'string'},
    TARGET_URI_FIELD: {'required': True, 'type': 'string'},
    TITLE_FIELD: {'required': True, 'type': 'string'}}


class MetadataIndexingTransformer(DataCatalogModel):

    """
    Validates and adjusts matadata entry fields in order to prepare them for indexation.
    """

    MISSING_FIELDS_ERROR_MESSAGE = ': missing fields in metadata entry.'
    NOT_VALID_URI_ERROR_MESSAGE = ': URI is not properly formed.'

    def transform(self, entry):
        """
        Executes the whole process of validation and adjustment of metadata entry.
        """
        self._validate_entry(entry)
        self._fill_out_creation_time(entry)

    def _validate_entry(self, entry):
        """
        Validation of the metadata entry.
        Rises an InvalidEntryError exception if the validation fails.
        """
        metadata_entry_validator = Validator(CERBERUS_SCHEMA)

        if not metadata_entry_validator.validate(entry):
            self._log.error(self.MISSING_FIELDS_ERROR_MESSAGE)
            raise InvalidEntryError(self.MISSING_FIELDS_ERROR_MESSAGE)

        url_parsed = urlparse(entry[TARGET_URI_FIELD])
        self._log.info("parsed url:" + str(url_parsed))
        if not url_parsed.scheme or not url_parsed.path or url_parsed.path == '/':
            self._log.error(self.NOT_VALID_URI_ERROR_MESSAGE)
            raise InvalidEntryError(self.NOT_VALID_URI_ERROR_MESSAGE)

    @staticmethod
    def _fill_out_creation_time(entry):
        """
        Creation time will be set to the current system time if it is not set.
        """
        if CREATION_TIME_FIELD not in entry:
            entry[CREATION_TIME_FIELD] = CURRENT_TIME_FUNCTION().isoformat()


class InvalidEntryError(Exception):

    def __init__(self, value):
        super(InvalidEntryError, self).__init__(value)
        self.value = value

    def __str__(self):
        return repr(self.value)


class MetadataEntryResource(DataCatalogResource):

    """
    Storage and retrieval of metadata describing a data set.
    """

    INDEX_ERROR_MESSAGE = 'Putting data set in index failed'
    MISSING_FIELDS_ERROR_MESSAGE = INDEX_ERROR_MESSAGE + ': missing fields in metadata entry.'
    MALFORMED_ERROR_MESSAGE = INDEX_ERROR_MESSAGE + ': malformed data in meta data fields.'
    NO_CONNECTION_ERROR_MESSAGE = INDEX_ERROR_MESSAGE + ': failed to connect to ElasticSearch.'

    def __init__(self):
        super(MetadataEntryResource, self).__init__()
        self._elastic_search = Elasticsearch(
            '{}:{}'.format(self._config.elastic.elastic_hostname,
                           self._config.elastic.elastic_port))
        self._parser = MetadataIndexingTransformer()
        self._dataset_delete = DataSetRemover()
        self._notifier = CFNotifier(self._config)

    def get(self, entry_id):
        """
        Gets a metadata entry labeled with the given ID.
        """
        if not flask.g.is_admin \
                and self._get_org_uuid(entry_id) not in flask.g.org_uuid_list \
                and not self._get_is_public_status(entry_id):
            self._log.warning('Forbidden access to the resource')
            return None, 403

        try:
            return self._elastic_search.get(
                index=self._config.elastic.elastic_index,
                doc_type=self._config.elastic.elastic_metadata_type,
                id=entry_id)
        except NotFoundError:
            self._log.exception('Data set with the given ID not found.')
            return None, 404
        except ConnectionError:
            self._log.exception('No connection to the index.')
            return None, 503

    def put(self, entry_id):
        """
        Puts a metadata entry in the search index under the given ID.
        """
        entry = flask.request.get_json(force=True)
        if not flask.g.is_admin and entry["orgUUID"] not in flask.g.org_uuid_list:
            self._log.warning('Forbidden access to the organisation')
            self._notify(entry, 'Forbidden access to the organisation')
            return None, 403

        try:
            self._log.info("processed entry: " + str(entry))
            self._parser.transform(entry)
        except InvalidEntryError as ex:
            self._log.error(ex.value)
            self._notify(entry, 'Error durning parsing entry')
            abort(400, ex.value)

        return self.add_data_set(entry_id, entry)

    def add_data_set(self, entry_id, entry):
        try:
            response = self._elastic_search.index(
                index=self._config.elastic.elastic_index,
                doc_type=self._config.elastic.elastic_metadata_type,
                id=entry_id,
                body=entry
            )
            self._notify(entry, 'Dataset added')
            if response['created']:
                return None, 201
            else:
                return None, 200
        except RequestError:
            self._log.exception(self.MALFORMED_ERROR_MESSAGE)
            self._notify(entry, self.MALFORMED_ERROR_MESSAGE)
            return None, 400
        except ConnectionError:
            self._log.exception(self.NO_CONNECTION_ERROR_MESSAGE)
            self._notify(entry, self.NO_CONNECTION_ERROR_MESSAGE)
            return None, 503

    def delete(self, entry_id):
        """
        Deletes a metadata entry labeled with the given ID.
        """
        entry = self._get_entry(entry_id)
        if not flask.g.is_admin and self._get_org_uuid(entry_id) not in flask.g.org_uuid_list:
            self._log.warning('Forbidden access to the resource')
            return None, 403
        token = flask.request.headers.get('Authorization')
        if not token:
            self._log.error('Authorization header not found.')
            return None, 401
        try:
            deletion_status = self._dataset_delete.delete(entry_id, token)
            self._notify(entry, "Dataset deleted")
            return deletion_status, 200
        except NotFoundError:
            self._log.exception('Data set with the given ID not found.')
            self._notify(entry, "Data set with the given ID not found.")
            return None, 404
        except ConnectionError:
            self._log.exception('No connection to the index.')
            self._notify(entry, 'No connection to the index.')
            return None, 503

    def post(self, entry_id):
        """
        Updates specified attributes of metadata entry with the given ID.
        The body of the POST method should be formed in a following way:

        {
            "argumentName": ["value01", "value02"]
        }

        The value of a given argument will replace current value for this argument
        in the specified metadata entry.

        Example:
        {
            "title": "A new, better title for this data set!"
        }
        """
        if not flask.g.is_admin and self._get_org_uuid(entry_id) not in flask.g.org_uuid_list:
            self._log.exception('Forbidden access to the resource')
            return None, 403
        exception_message = "Failed to update the data set's attributes."

        body = flask.request.get_json(force=True)
        if not set(body).issubset(CERBERUS_SCHEMA):
            self._log.warn('Request body is invalid. Data: %s', flask.request.data)
            abort(400)
        body_dict = {'doc': body}

        try:
            if 'isPublic' in body:
                token = self._get_token_from_request()
                self._dataset_delete.delete_public_from_hive(entry_id, token)
        except NotFoundError:
            self._log.exception('Data set with the given ID not found.')
            return None, 404
        except ConnectionError:
            self._log.exception('No connection to the index.')
            return None, 503

        try:
            self._elastic_search.update(
                index=self._config.elastic.elastic_index,
                doc_type=self._config.elastic.elastic_metadata_type,
                id=entry_id,
                body=body_dict)
            is_public_status_tag = 'public' if self._get_is_public_status(entry_id) else 'private'
            self._notify(self._get_entry(entry_id),
                         "Dataset changed status on",
                         is_public_status_tag)
        except NotFoundError:
            self._log.exception(exception_message)
            self._notify(self._get_entry(entry_id), exception_message)
            abort(404)
        except ConnectionError:
            self._log.exception('No connection to the index.')
            self._notify(self._get_entry(entry_id), 'No connection to the index.')
            return None, 503

        return

    def _notify(self, entry, message, status=""):
        """
        helper function for formating notifier messages
        """
        notify_msg = '{} - {} {}'.format(entry.get('sourceUri', ''), message, status)
        self._notifier.notify(notify_msg, entry['orgUUID'])

    def _get_org_uuid(self, entry_id):
        return self._get_entry(entry_id)["orgUUID"]

    def _get_is_public_status(self, entry_id):
        return self._get_entry(entry_id)["isPublic"]

    def _get_entry(self, entry_id):
        try:
            return self._elastic_search.get(
                index=self._config.elastic.elastic_index,
                doc_type=self._config.elastic.elastic_metadata_type,
                id=entry_id)["_source"]

        except NotFoundError:
            self._log.exception("Not found")
            abort(404)

    def _get_token_from_request(self):
        token = flask.request.headers.get('Authorization')
        if not token:
            self._log.error('Authorization header not found.')
            return None, 401
        return token
