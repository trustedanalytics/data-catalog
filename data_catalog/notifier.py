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

import time
import json
import pynats
import logging

class CFNotifier(object):

    """
    Class responsible for notifing data-catalog actions
    to NAT's service.
    """

    def __init__(self, config):
        self._connection = pynats.Connection(url=config.services_url.nats_url, verbose=True)
        self._subject = config.services_url.nats_subject
        self._log = logging.getLogger(type(self).__name__)

        self._log.info(
            'CloudFoundry notifier will talk to NATS at %s on subject %s',
            config.services_url.nats_url,
            config.services_url.nats_subject
        )

    def notify(self, message, org_guid):
        """
        send message to NAT's service
        :param message: message to send
        :param org_guid: organization guid to which this message is connected
        """
        self._connection.connect()
        nats_message = self._create_message(message, org_guid)
        self._connection.publish(self._subject, json.dumps(nats_message))

    @staticmethod
    def _create_message(message, org_guid):
        return {'OrgGuid': org_guid,
                'Message': message,
                'Timestamp': CFNotifier._get_timestamp()}

    @staticmethod
    def _get_timestamp():
        return int(time.time()*1000)

