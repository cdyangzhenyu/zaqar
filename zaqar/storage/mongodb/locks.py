# Copyright (c) 2017 Eayun, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy
# of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""
Schema:
  'k': lock_key :: six.text_type
  's': status :: six.text_type
  'e': lock_expires_time :: datetime
"""

import datetime

from oslo_log import log as logging
from oslo_utils import timeutils

from zaqar.storage import base
from zaqar.storage.mongodb import utils


LOG = logging.getLogger(__name__)

# For removing expired lock
TTL_INDEX_FIELDS = [
    ('e', 1),
]

LOCKS_INDEX = [
    ('k', 1),
]

LOCKS_STATUS_INDEX = [
    ('k', 1),
    ('s', 1),
]

OMIT_FIELDS = (('_id', False),)

LOCK_TTL = {
    'purge': 60
}


def _field_spec():
    return dict(OMIT_FIELDS)


def _get_key(name, r_type, project, operation):
    return '%s/%s/%s/%s' % (project, r_type, name, operation)


class LockController(base.LockBase):

    def __init__(self, *args, **kwargs):
        super(LockController, self).__init__(*args, **kwargs)
        self._queue_ctrl = self.driver.queue_controller
        self._col = self.driver.lock_database.locks
        self._col.ensure_index(TTL_INDEX_FIELDS,
                               name='ttl',
                               expireAfterSeconds=0,
                               background=True)
        self._col.ensure_index(LOCKS_INDEX,
                               background=True,
                               name='locks_key',
                               unique=True)
        self._col.ensure_index(LOCKS_STATUS_INDEX,
                               background=True,
                               name='locks_status_key',
                               unique=True)

    @utils.raises_conn_error
    def get(self, name, r_type, project, operation):
        key = _get_key(name, r_type, project, operation)
        now = timeutils.utcnow_ts()
        dt = datetime.datetime.utcfromtimestamp(now + LOCK_TTL[operation])
        try:
            self._col.update({'k': key, 's': {'$ne': 'lock'}},
                             {'$set': {'s': 'lock', 'e': dt}},
                             upsert=True)
        except Exception as ex:
            LOG.debug(ex)
            return False

        return True

    @utils.raises_conn_error
    def release(self, name, r_type, project, operation):
        key = _get_key(name, r_type, project, operation)
        self._col.update({'k': key, 's': 'lock'},
                         {'$set': {'s': 'release'}})

    @utils.raises_conn_error
    def is_locked(self, name, r_type, project, operation):
        key = _get_key(name, r_type, project, operation)
        one = self._col.find_one({'k': key, 's': 'lock'},
                                 _field_spec())
        return True if one else False
