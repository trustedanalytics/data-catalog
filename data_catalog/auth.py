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
import requests
import flask
from werkzeug.exceptions import BadRequest
from flask_restful import abort
import jwt
import jwt.exceptions

from data_catalog.configuration import DCConfig


class Security(object):

    def __init__(self, auth_exceptions):
        """
        :param auth_exceptions: request paths that won't be subject to authorization process
        :type auth_exceptions: list[str]
        """
        self._log = logging.getLogger(type(self).__name__)
        self._authorization = _Authorization()
        self.auth_exceptions = auth_exceptions
        self._uaa_public_key = None
        self._uaa_sign_algorithm = None

    def authenticate(self):
        """
        Verifies user's token and his/her accessibility to requested resources.
        Once token is validated, the role of user (flask.g.is_admin) and his/her scope
        (flask.g.org_uuid_list) is set up for current request.
        Raises Unauthorized when token is missing, invalid, expired or not signed by UAA
        Raises Forbidden: when org guid is missing, invalid or user can't access this org
        """
        if not self._uaa_public_key:
            self._get_token_verification_key()

        if any(exc in str(flask.request.path) for exc in self.auth_exceptions):
            return

        try:
            token = self._get_token_from_request()
            token_payload = self._parse_auth_token(token)
        except (_MissingAuthToken, jwt.InvalidTokenError) as ex:
            self._log.warn(str(ex))
            abort(401)

        flask.g.is_admin = self._is_admin(token_payload)
        try:
            flask.g.org_uuid_list = self._authorization.get_user_scope(
                token,
                flask.request,
                flask.g.is_admin)
        except (_InvalidOrgId, _CloudControllerConnectionError, _UserCantAccessOrg):
            self._log.exception('Failed to authenticate the user.')
            abort(403)

    def _get_token_verification_key(self):
        uaa_public_key = requests.get(DCConfig().services_url.uaa_token_uri).json()
        self._uaa_public_key, self._uaa_sign_algorithm = _PublicKeyParser().parse(uaa_public_key)

    def _get_token_from_request(self):
        self._log.debug('headers ' + str(flask.request.headers))

        auth_header = flask.request.headers.get('Authorization')
        if auth_header is None:
            raise _MissingAuthToken('Authorization header not found.')

        # TODO verify that it's a bearer token
        return auth_header.split()[1]

    def _parse_auth_token(self, token):
        token_payload = jwt.decode(token, key=self._uaa_public_key, verify=True,
                                   algorithms=['RS256'], audience="cloud_controller")
        self._log.debug('token_payload ' + str(token_payload))
        return token_payload

    @staticmethod
    def _is_admin(token_payload):
        return 'console.admin' in token_payload['scope']


class _PublicKeyParser(object):
    ALGORITHMS = {
        'HS256': 'HS256', 'SHA256WITHHMAC': 'HS256',
        'HS384': 'HS384', 'SHA384WITHHMAC': 'HS384',
        'HS512': 'HS512', 'SHA512WITHHMAC': 'HS512',
        'ES256': 'ES256', 'SHA256WITHECDSA': 'ES256',
        'ES384': 'ES384', 'SHA384WITHECDSA': 'ES384',
        'ES512': 'ES512', 'SHA512WITHECDSA': 'ES512',
        'RS256': 'RS256', 'SHA256WITHRSA': 'RS256',
        'RS384': 'RS384', 'SHA384WITHRSA': 'RS384',
        'RS512': 'RS512', 'SHA512WITHRSA': 'RS512',
    }

    def parse(self, public_key):
        key = public_key['value']
        alg = self._decode_token_sign_alg(public_key['alg'])
        return key, alg

    def _decode_token_sign_alg(self, alg):
        alg = alg.upper()
        if alg in self.ALGORITHMS:
            return self.ALGORITHMS.get(alg)
        else:
            raise Exception('"{}" is not on the list of known algorithms: {}'
                            .format(alg, str(self.ALGORITHMS.keys())))


class _Authorization(object):

    def __init__(self):
        self._log = logging.getLogger(type(self).__name__)
        self._config = DCConfig()

    def get_user_scope(self, token, request, is_admin):
        requested_orgs = self._get_requested_orgs(request)
        user_orgs = self._get_orgs_user_has_access(token)
        self._log.debug('User belongs to orgs: {}/nUser requested access to: {}'
                        .format(user_orgs, requested_orgs))
        if is_admin:
            return requested_orgs

        if requested_orgs:
            if set(requested_orgs).issubset(set(user_orgs)):
                return requested_orgs
            else:
                raise _UserCantAccessOrg(
                    'User is not authorized to access at least some of these organizations: {}'
                    .format(requested_orgs))
        else:
            return user_orgs

    def _get_requested_orgs(self, request):
        """
        :param request: Flask request object
        :return: Names of organizations the user belongs to.
        :rtype: list[str]
        """
        if request.method == 'GET':
            orgs_string = request.args.get('orgs', default="", type=str)
            return [uuid.lower().strip() for uuid in orgs_string.split(',')] if orgs_string else []
        elif request.method in ['PUT', 'POST']:
            try:
                org_string = request.get_json(force=True).get('orgUUID', '')
                return [org_string.lower()] if org_string else []
            except BadRequest as ex:
                self._log.debug("Error getting organizations, using empty set. Error: %s", str(ex))
                return []
        else:
            return []

    def _get_orgs_user_has_access(self, token):
        response = requests.get(
            self._config.services_url.user_management_uri,
            headers={'Authorization': 'bearer {}'.format(token)})
        self._handle_downloader_status_code(response.status_code)
        org_uuid_list = []
        for org in json.loads(response.text):
            org_uuid_list.append(org['organization']['metadata']['guid'])
        return org_uuid_list

    @staticmethod
    def _handle_downloader_status_code(status_code):
        if status_code == 200:
            return
        elif status_code == 401:
            raise _TokenNotFoundOrExpired()
        elif status_code == 404:
            raise _NotFoundInExternalService()
        raise _UserManagementServiceError("Error while accessing user management service."
                                          "Status code: {}".format(status_code))


class _UserManagementServiceError(Exception):
    pass


class _NotFoundInExternalService(Exception):
    pass


class _TokenNotFoundOrExpired(Exception):
    pass


class _MissingAuthToken(Exception):
    pass


class _InvalidOrgId(Exception):
    pass


class _CloudControllerConnectionError(Exception):
    pass


class _UserCantAccessOrg(Exception):
    pass
