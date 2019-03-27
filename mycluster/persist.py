
from builtins import str
from builtins import object
from persistent import Persistent
from ZODB import FileStorage, DB
from .mycluster import get_directory, scheduler
# from BTrees.OOBTree import OOBTree
import transaction
import logging


class JobDB(object):

    def __init__(self):

        directory = get_directory()

        self.logger = logging.getLogger('ZODB.FileStorage')
        fh = logging.FileHandler(directory + 'db.log')
        self.logger.addHandler(fh)

        self.storage = FileStorage.FileStorage(directory + 'db.fs')
        self.db = DB(self.storage)
        self.connection = self.db.open()

        dbroot = self.connection.root()

        if 'job_key' not in dbroot:
            from BTrees.OOBTree import OOBTree
            dbroot['job_key'] = OOBTree()
            dbroot['job_key']['val'] = 0

        self.job_key = dbroot['job_key']

        # Ensure that a 'job_db' key is present
        # in the root
        if 'job_db' not in dbroot:
            from BTrees.OOBTree import OOBTree
            dbroot['job_db'] = OOBTree()

        self.job_db = dbroot['job_db']

        if 'user_db' not in dbroot:
            from BTrees.OOBTree import OOBTree
            dbroot['user_db'] = OOBTree()
            self.user_db = dbroot['user_db']
            self.user_db['user'] = User('unknown', 'unknown', 'unknown')

        self.user_db = dbroot['user_db']

        if 'site_db' not in dbroot:
            from BTrees.OOBTree import OOBTree
            dbroot['site_db'] = OOBTree()
            self.site_db = dbroot['site_db']

        self.site_db = dbroot['site_db']

        if scheduler is not None:
            self.site_db[scheduler.name()] = Site(scheduler.name(),
                                                  scheduler.scheduler_type())

        if 'queue_db' not in dbroot:
            from BTrees.OOBTree import OOBTree
            dbroot['queue_db'] = OOBTree()
            self.queue_db = dbroot['queue_db']

        self.queue_db = dbroot['queue_db']

        from .version import get_git_version
        if 'version' not in dbroot:
            dbroot['version'] = get_git_version()
        else:
            current_version = dbroot['version']
            new_version = get_git_version()
            # Add any migrations required here
            if current_version != new_version:
                pass

            dbroot['version'] = new_version

        if 'remote_site_db' not in dbroot:
            from BTrees.OOBTree import OOBTree
            dbroot['remote_site_db'] = OOBTree()
            self.remote_site_db = dbroot['remote_site_db']

        self.remote_site_db = dbroot['remote_site_db']

    def add_job(self, job):
        # Get unique key
        job_key = self.job_key['val']
        self.job_key['val'] = job_key + 1
        transaction.commit()
        # Add job
        self.job_db[job_key] = job
        transaction.commit()

    def add_queue(self, queue, site):
        if queue not in self.queue_db:
            self.queue_db[queue] = Queue(queue, site)
            transaction.commit()

    def add_remote(self, remote_site):
        if remote_site not in self.remote_site_db:
            user = remote_site.split('@')[0]
            site = remote_site.split('@')[1]
            self.remote_site_db[site] = RemoteSite(site, user)
            transaction.commit()

    def get(self, job_id):
        # Find
        for key in list(self.job_db.keys()):
            if self.job_db[key].job_id == job_id:
                return self.job_db[key]

        raise KeyError('Key: ' + str(job_id) + ' not found')

    def list(self):
        pass

    def commit(self):
        transaction.commit()

    def close(self):
        self.connection.close()
        self.db.close()
        self.storage.close()


class RemoteSite(Persistent):

    def __init__(self, name, user):
        self.name = name
        self.user = user


class Site(Persistent):

    def __init__(self, name, scheduler_type):
        self.name = name
        self.PUE = 0.0
        self.total_num_nodes = 0
        self.total_storage_power = 0.0
        self.total_network_power = 0.0
        self.total_management_power = 0.0
        self.scheduler_type = scheduler_type


class Queue(Persistent):

    def __init__(self, name, site_name):
        self.name = name
        self.site_name = site_name
        self.node_power = 0.0
        self.node_charge = 0.0


class User(Persistent):

    def __init__(self, first_name, last_name, email):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email

    def update_email(self, email):
        self.email = email
        transaction.commit()

    def firstname(self, name):
        self.first_name = name
        transaction.commit()

    def lastname(self, name):
        self.last_name = name
        transaction.commit()


class Job(Persistent):

    def __init__(self, job_id, time_stamp):
        self.job_id = job_id
        self.time_stamp = time_stamp
        self.status = 'submitted'
        self.num_tasks = 0
        self.tasks_per_node = 0
        self.threads_per_task = 0
        self.num_nodes = 0
        self.stats = {}
        self.sysscribe = None
        self.app_name = 'unknown'
        self.app_data_metric = 0
        self.job_script_name = 'unknown'
        self.job_name = 'unknown'
        self.job_dir = 'unknown'
        self.queue = 'unknown'

    def update_status(self, new_status):

        self.status = new_status
        transaction.commit()

    def update_sysscribe(self, sys_dict):
        self.sysscribe = sys_dict
        self._p_changed = True
        transaction.commit()

    def update_stats(self, stats):
        self.stats = stats
        self._p_changed = True
        transaction.commit()

    def appname(self, name):
        self.app_name = name
        transaction.commit()

    def appdata(self, data):

        self.app_data_metric = data
        transaction.commit()
