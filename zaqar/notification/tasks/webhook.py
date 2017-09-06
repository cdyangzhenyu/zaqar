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
import time
import random
from zaqar.i18n import _LE

LOG = logging.getLogger(__name__)


class WebhookTask(object):

    def execute(self, subscription, messages, headers=None, **kwargs):
        if headers is None:
            headers = {'Content-Type': 'application/json'}
        headers.update(subscription['options'].get('post_headers', {}))
        retry_policy = subscription['options'].get('push_policy', None)
        conf = kwargs.get('conf', None)
        project = kwargs.get('project', None)
        monitor_controller = kwargs.get('monitor_controller', None)
        def _post_msg():
            for msg in messages:
                # NOTE(Eva-i): Unfortunately this will add 'queue_name' key to
                # our original messages(dicts) which will be later consumed in
                # the storage controller. It seems safe though.
                msg['topic_name'] = subscription['source']
                if 'post_data' in subscription['options']:
                    data = subscription['options']['post_data']
                    data = data.replace('"$zaqar_message$"', json.dumps(msg))
                else:
                    data = json.dumps(msg)
                requests.post(subscription['subscriber'],
                              data=data,
                              headers=headers)
                try:
                    monitor_controller.update(messages, subscription['source'],
                                              project, 'subscribe_messages', success=True)
                except Exception as ex:
                    LOG.exception(ex)

        try:
            _post_msg()
            pass
        except Exception as e:
            LOG.exception(_LE('webhook task got exception: %s.') % str(e))
            if retry_policy == 'BACKOFF_RETRY':
                for i in range(3):
                    sleep_time = random.randint(10,20)
                    time.sleep(sleep_time)
                    LOG.debug('Retry_policy[BACKOFF_RETRY]: retry times: %s, sleep time: %ss,'
                              'The subscription is: %s, The messages is: %s,' %
                              (i + 1, sleep_time, subscription, messages))
                    try:
                        _post_msg()
                        break
                    except Exception as e:
                        LOG.debug(_LE('webhook task retry got exception: %s.') % str(e))
                else:
                    pass
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
                        _post_msg()
                        break
                    except Exception as e:
                        LOG.debug(_LE('webhook task retry got exception: %s.') % str(e))
                else:
                    pass

    def register(self, subscriber, options, ttl, project_id, request_data):
        pass
