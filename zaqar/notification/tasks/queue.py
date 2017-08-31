# Copyright (c) 2015 Catalyst IT Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from oslo_log import log as logging
import requests

from zaqar.i18n import _LE

LOG = logging.getLogger(__name__)


class QueueTask(object):

    def execute(self, subscription, messages, **kwargs):
        queue_name = subscription.get('subscriber', '').split(':')[-1]
        client_uuid = kwargs.get('client_uuid', None)
        message_controller = kwargs.get('message_controller', None)
        queue_controller = kwargs.get('queue_controller', None)
        project_id = kwargs.get('project', None)
        queue_meta = queue_controller.get_metadata(queue_name, project_id)
        queue_max_msg_size = queue_meta.get('_max_messages_post_size', 65535)
        queue_default_ttl = queue_meta.get('_default_message_ttl', 3600)
        delay_ttl = queue_meta.get('delay_ttl', 0)
        new_messages = []
        for msg in messages:
            new_msg = {}
            new_msg['ttl'] = queue_default_ttl
            new_msg['delay_ttl'] = delay_ttl
            new_msg['body'] = msg['body']
            new_messages.append(new_msg)
        try:
            message_ids = message_controller.post(
                                            queue_name,
                                            messages=new_messages,
                                            project=project_id,
                                            client_uuid=client_uuid)
            LOG.debug('Messages: %s publish for Subscription: %s Success. Message id is: %s ' %
                  (messages, subscription, message_ids))
        except Exception as e:    
            LOG.exception(_LE('queue task got exception: %s.') % str(e))
            retry_policy = subscription['options'].get('push_policy', None)
            conf = kwargs.get('conf', None)
            if retry_policy == 'BACKOFF_RETRY':
                for i in range(3):
                    sleep_time = random.randint(10,20)
                    time.sleep(sleep_time)
                    LOG.debug('Retry_policy[BACKOFF_RETRY]: retry times: %s, sleep time: %ss,'
                              'The subscription is: %s, The messages is: %s,' %
                              (i + 1, sleep_time, subscription, messages))
                    try:
                        message_ids = message_controller.post(
                                            queue_name,
                                            messages=new_messages,
                                            project=project_id,
                                            client_uuid=client_uuid)
                        break
                    except Exception as e:
                        LOG.debug(_LE('webhook task retry got exception: %s.') % str(e))
            elif retry_policy == 'EXPONENTIAL_DECAY_RETRY':
                for i in range(conf.notification.max_notifier_retries):
                    sleep_time = 2**i
                    if sleep_time > 512:
                        sleep_time = 512
                    time.sleep(sleep_time)
                    LOG.debug('Retry_policy[EXPONENTIAL_DECAY_RETRY]: retry times: %s, sleep time: %ss,'
                              'The subscription is: %s, The messages is: %s,' %
                              (i + 1, sleep_time, subscription, messages))
                    try:
                        message_ids = message_controller.post(
                                            queue_name,
                                            messages=new_messages,
                                            project=project_id,
                                            client_uuid=client_uuid)
                        break
                    except Exception as e:
                        LOG.debug(_LE('webhook task retry got exception: %s.') % str(e))

    def register(self, subscriber, options, ttl, project_id, request_data):
        pass
