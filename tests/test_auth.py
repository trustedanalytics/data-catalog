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

import unittest
import jwt
import flask

import StringIO

from ddt import ddt, data, unpack
from mock import patch, MagicMock
from werkzeug.exceptions import Unauthorized, Forbidden

from data_catalog.auth import Security, _Authorization, _UserCantAccessOrg, _InvalidOrgId
from tests.base_test import DataCatalogTestCase

TEST_UAA_KEY = 'test_key', 'test_alg'
TEST_TOKEN = 'test_token'
TEST_USER_ID = 'test_user_id'
TEST_ORG_UUID = ['test_org_uuid']
TEST_TOKEN_PAYLOAD = 'test_token_payload'


class AuthTests(DataCatalogTestCase):

    def setUp(self):
        super(AuthTests, self).setUp()
        self.security = Security(['/api/spec'])
        self.security._get_token_from_request = MagicMock(return_value=TEST_TOKEN)
        self.security._is_admin = MagicMock(return_value=False)
        self.security._uaa_public_key = 'fake_test_key'
        self.request_context = self.app.test_request_context(u'/fake_call_path')

    @patch.object(Security, '_parse_auth_token', return_value=TEST_TOKEN_PAYLOAD)
    @patch.object(_Authorization, 'get_user_scope', return_value=TEST_ORG_UUID)
    def test_authenticate_userHasAccess_authenticateSuccessful(self, mock_get_scope, mock_parse_token):
        with self.request_context:
            actual_result = self.security.authenticate()
            self.assertEquals(actual_result, None)
            self.security._get_token_from_request.assert_called_with()
            mock_parse_token.assert_called_with(TEST_TOKEN)
            mock_get_scope.assert_called_with(TEST_TOKEN, flask.request, False)

    @patch.object(Security, '_parse_auth_token', side_effect=jwt.InvalidTokenError())
    def test_authenticate_verifyTokenFails_return401(self, mock_parse_token):
        with self.request_context:
            self.assertRaises(Unauthorized, self.security.authenticate)
            self.security._get_token_from_request.assert_called_with()
            mock_parse_token.assert_called_with(TEST_TOKEN)

    @patch.object(Security, '_parse_auth_token', return_value=TEST_USER_ID)
    @patch.object(_Authorization, 'get_user_scope', side_effect=_UserCantAccessOrg())
    def test_authenticate_verifyAccessFails_return403(self, mock_get_scope, mock_parse_token):
        with self.request_context:
            self.assertRaises(Forbidden, self.security.authenticate)
            self.security._get_token_from_request.assert_called_with()
            mock_parse_token.assert_called_with(TEST_TOKEN)
            mock_get_scope.assert_called_with(TEST_TOKEN, flask.request, False)

    def test_authenticate_requestFromSwagger_authenticationIgnored(self):
        swagger_request_context = self.app.test_request_context("/api/spec/get_swagger_endpoints")
        with swagger_request_context:
            actual_result = self.security.authenticate()
            self.assertEquals(actual_result, None)


@ddt
class AuthorizationTests(DataCatalogTestCase):
    def setUp(self):
        super(AuthorizationTests, self).setUp()
        self.authorization = _Authorization()

    @data(([], [], u'/fake_path', 'GET', '', False),
          (['org1', 'org2'], [u'org1', u'org2'], u'/fake_path', 'GET', '', False),
          (['org1', 'org2'], [u'org1'], u'/fake_path?orgs=org1', 'GET', '', False),
          (['org1', 'org2'], [u'org1', u'org2'], u'/fake_path?orgs=org1,oRG2', 'GET', '', False),
          (['org1'], [u'org1'], u'/fake_path', 'PUT', '{"orgUUID": "org1"}', False),
          (['org1', 'org2'], [u'org2'], u'/fake_path', 'POST', '{"orgUUID": "org2"}', False),
          ([], [u'org1', u'org2'], u'/fake_path?orgs=org1,org2', 'GET', '', True),
          ([], [u'org1'], u'/fake_path', 'PUT', '{"orgUUID": "org1"}', True))
    @unpack
    def test_checkUserAccess_properAccessRights_properPrivilagesReturned(
            self,
            user_orgs,
            requested_orgs,
            request_path,
            method,
            request_body,
            is_admin):
        self.authorization._get_orgs_user_has_access = MagicMock(return_value=user_orgs)
        request_context = self.app.test_request_context(
            request_path,
            method=method,
            input_stream=StringIO.StringIO(request_body))

        with request_context:
            self.assertEquals(
                requested_orgs,
                self.authorization.get_user_scope('fake_token', flask.request, is_admin)
            )

    @data(([], u'/fake_path?orgs=org1', 'GET', ''),
          (['org1', 'org2'], u'/fake_path?orgs=org1,org3', 'GET', ''),
          (['org1'], u'/fake_path', 'PUT', '{"orgUUID": "org2"}'),
          ([], u'/fake_path', 'POST', '{"orgUUID": "org1"}'))
    @unpack
    def test_checkUserAccess_notEnoughOrgAccess_raiseNoAccessError(
            self,
            user_orgs,
            request_path,
            method,
            request_body):
        self.authorization._get_orgs_user_has_access = MagicMock(return_value=user_orgs)
        request_context = self.app.test_request_context(
            request_path,
            method=method,
            input_stream=StringIO.StringIO(request_body))

        with request_context:
            with self.assertRaises(_UserCantAccessOrg):
                self.authorization.get_user_scope('fake_token', flask.request, False)
