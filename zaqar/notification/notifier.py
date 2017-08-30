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

import enum
from stevedore import driver

import futurist
from oslo_log import log as logging
from six.moves import urllib_parse

from zaqar.common import auth
from zaqar.common import urls
from zaqar.i18n import _LE
from zaqar.i18n import _LI
from zaqar.storage import pooling

LOG = logging.getLogger(__name__)


@enum.unique
class MessageType(enum.IntEnum):
    """Enum of message type."""
    SubscriptionConfirmation = 1
    UnsubscribeConfirmation = 2
    Notification = 3


class NotifierDriver(object):
    """Notifier which is responsible for sending messages to subscribers.

    """

    def __init__(self, *args, **kwargs):
        self.subscription_controller = kwargs.get('subscription_controller')
        max_workers = kwargs.get('max_notifier_workers', 10)
        self.executor = futurist.ThreadPoolExecutor(max_workers=max_workers)
        self.require_confirmation = kwargs.get('require_confirmation', False)

    def publish(self, topic_name, messages, client_uuid, project=None):
        """Send messages to the subscribers."""
        if self.subscription_controller:
            if not isinstance(self.subscription_controller,
                              pooling.SubscriptionController):
                marker = None
                while True:
                    subscribers = self.subscription_controller.list(
                        topic_name, project, marker=marker)
                    for sub in next(subscribers):
                        LOG.debug("Notifying subscriber %r" % (sub,))
                        s_type = urllib_parse.urlparse(
                            sub['subscriber']).scheme
                        # If the subscriber doesn't contain 'confirmed', it
                        # means that this kind of subscriber was created before
                        # the confirm feature be introduced into Zaqar. We
                        # should allow them be subscribed.
                        if (self.require_confirmation and
                                not sub.get('confirmed', True)):
                            LOG.info(_LI('The subscriber %s is not '
                                         'confirmed.'), sub['subscriber'])
                            continue
                        # Note(yangzhenyu): If the subscriber has tags and it
                        # doesn't contain the message tags, We should not allow
                        # the message be subscribed.
                        sub_tags = sub.get('options', {}).get('tags', [])
                        publish_msgs = []
                        for msg in messages:
                            msg_tags = msg.get('tags', [])
                            if not sub_tags or list(set(sub_tags).intersection(set(msg_tags))):
                                msg['Message_Type'] = MessageType.Notification.name
                                publish_msgs.append(msg)
                        self._execute(s_type, sub, publish_msgs)
                    marker = next(subscribers)
                    if not marker:
                        break
        else:
            LOG.error(_LE('Failed to get subscription controller.'))

    def send_confirm_notification(self, queue, subscription, conf,
                                  project=None, expires=None,
                                  api_version=None, is_unsubscribed=False):
        # NOTE(flwang): If the confirmation feature isn't enabled, just do
        # nothing. Here we're getting the require_confirmation from conf
        # object instead of using self.require_confirmation, because the
        # variable from self object really depends on the kwargs when
        # initializing the NotifierDriver object. See bug 1655812 for more
        # information.
        if not conf.notification.require_confirmation:
            return

        key = conf.signed_url.secret_key
        if not key:
            LOG.error(_LE("Can't send confirm notification due to the value of"
                          " secret_key option is None"))
            return
        url = "/%s/queues/%s/subscriptions/%s/confirm" % (api_version, queue,
                                                          subscription['id'])
        pre_url = urls.create_signed_url(key, [url], project=project,
                                         expires=expires, methods=['PUT'])
        message = None
        if is_unsubscribed:
            message_type = MessageType.UnsubscribeConfirmation.name
            message = ('You have unsubscribed successfully to the queue: %s, '
                       'you can resubscribe it by using confirmed=True.'
                       % queue)
        else:
            message_type = MessageType.SubscriptionConfirmation.name
            message = 'You have chosen to subscribe to the queue: %s' % queue

        messages = {}
        endpoint_dict = auth.get_public_endpoint()
        if endpoint_dict:
            wsgi_endpoint = endpoint_dict.get('zaqar', None)
            if wsgi_endpoint:
                wsgi_subscribe_url = urllib_parse.urljoin(
                    wsgi_endpoint, url)
                messages['WSGISubscribeURL'] = wsgi_subscribe_url
            websocket_endpoint = endpoint_dict.get('zaqar-websocket', None)
            if websocket_endpoint:
                websocket_subscribe_url = urllib_parse.urljoin(
                    websocket_endpoint, url)
                messages['WebSocketSubscribeURL'] = websocket_subscribe_url
        messages.update({'Message_Type': message_type,
                         'Message': message,
                         'URL-Signature': pre_url['signature'],
                         'URL-Methods': pre_url['methods'][0],
                         'URL-Paths': pre_url['paths'][0],
                         'X-Project-ID': pre_url['project'],
                         'URL-Expires': pre_url['expires'],
                         'SubscribeBody': {'confirmed': True},
                         'UnsubscribeBody': {'confirmed': False}})
        s_type = urllib_parse.urlparse(subscription['subscriber']).scheme
        LOG.info(_LI('Begin to send %(type)s confirm/unsubscribe notification.'
                     ' The request body is %(messages)s'),
                 {'type': s_type, 'messages': messages})

        self._execute(s_type, subscription, [messages], conf)

    def _execute(self, s_type, subscription, messages, conf=None):
        if self.subscription_controller:
            data_driver = self.subscription_controller.driver
            conf = data_driver.conf
        else:
            conf = conf
        mgr = driver.DriverManager('zaqar.notification.tasks',
                                   s_type,
                                   invoke_on_load=True)
        self.executor.submit(mgr.driver.execute, subscription, messages,
                             conf=conf)
