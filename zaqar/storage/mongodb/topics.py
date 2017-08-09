# Copyright (c) 2013 Red Hat, Inc.
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

"""Implements the MongoDB storage controller for topics.

Field Mappings:
    In order to reduce the disk / memory space used,
    field names will be, most of the time, the first
    letter of their long name.
"""

from oslo_log import log as logging
from oslo_utils import timeutils
import pymongo.errors

from zaqar.common import decorators
from zaqar.i18n import _
from zaqar import storage
from zaqar.storage import errors
from zaqar.storage.mongodb import utils

LOG = logging.getLogger(__name__)

_TOPIC_CACHE_TTL = 5
_TOPIC_CACHE_PREFIX = 'topiccontroller:'

def _topic_exists_key(topic, project=None):
    return _TOPIC_CACHE_PREFIX + 'exists:' + str(project) + '/' + topic

class TopicController(storage.Topic):
    """Implements topic resource operations using MongoDB.

    Topics are scoped by project, which is prefixed to the
    topic name.

    ::

        Topics:

            Name            Field
            ---------------------
            name         ->   p_t
            msg counter  ->     c
            metadata     ->     m

        Message Counter:

            Name          Field
            -------------------
            value        ->   v
            modified ts  ->   t
    """

    def __init__(self, *args, **kwargs):
        super(TopicController, self).__init__(*args, **kwargs)

        self._cache = self.driver.cache
        self._collection = self.driver.topics_database.topics
        self._collection.ensure_index([('p_t', 1)], unique=True)

    # ----------------------------------------------------------------------
    # Interface
    # ----------------------------------------------------------------------
    
    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @decorators.caches(_topic_exists_key, _TOPIC_CACHE_TTL, lambda v: v)
    def _exists(self, name, project=None):
        query = _get_scoped_query(name, project)
        return self._collection.find_one(query) is not None
    
    def _get(self, name, project=None):
        try:
            return self.get_metadata(name, project)
        except errors.TopicDoesNotExist:
            return {}
        
    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def get_metadata(self, name, project=None):
        topic = self._collection.find_one(_get_scoped_query(name, project),
                                          projection={'m': 1, '_id': 0})
        if topic is None:
            raise errors.TopicDoesNotExist(name, project)

        return topic.get('m', {})

    def _list(self, project=None, marker=None,
              limit=storage.DEFAULT_TOPICS_PER_PAGE, detailed=False):

        query = utils.scoped_query(marker, project, key='p_t')

        projection = {'p_t': 1, '_id': 0}
        if detailed:
            projection['m'] = 1

        cursor = self._collection.find(query, projection=projection)
        cursor = cursor.limit(limit).sort('p_t')
        marker_name = {}

        def normalizer(record):
            topic = {'name': utils.descope_name(record['p_t'])}
            marker_name['next'] = topic['name']
            if detailed:
                topic['metadata'] = record['m']
            return topic

        yield utils.HookedCursor(cursor, normalizer)
        yield marker_name and marker_name['next']
        
    @utils.raises_conn_error
    def _create(self, name, metadata=None, project=None):
        try:
            counter = {'v': 1, 't': 0}

            scoped_name = utils.scope_name(name, project)
            self._collection.insert({'p_t': scoped_name, 'm': metadata or {},
                                     'c': counter})

        except pymongo.errors.DuplicateKeyError:
            return False
        else:
            return True
        
    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @_exists.purges
    def _delete(self, name, project=None):
        self._collection.remove(_get_scoped_query(name, project))
    
        
def _get_scoped_query(name, project):
    return {'p_t': utils.scope_name(name, project)}