import ZODB
from persistent import Persistent
from ZODB import FileStorage, DB
from mycluster import get_directory,scheduler
from BTrees.OOBTree import OOBTree
import transaction
import logging


class JobDB(object):
        
    def __init__(self):
    
        directory = get_directory()
        
        self.logger = logging.getLogger('ZODB.FileStorage')
        fh = logging.FileHandler(directory+'db.log')
        self.logger.addHandler(fh)
        
        self.storage = FileStorage.FileStorage(directory+'db.fs')
        self.db = DB(self.storage)
        self.connection = self.db.open()
        
        dbroot = self.connection.root()

        # Ensure that a 'job_db' key is present
        # in the root
        if not dbroot.has_key('job_db'):
            from BTrees.OOBTree import OOBTree
            dbroot['job_db'] = OOBTree()

        self.job_db = dbroot['job_db']

        if not dbroot.has_key('user_db'):
            from BTrees.OOBTree import OOBTree
            dbroot['user_db'] = OOBTree()
            self.user_db = dbroot['user_db']
            self.user_db['user'] = User('unknown','unknown','unknown')
            
        self.user_db = dbroot['user_db']

        if not dbroot.has_key('site_db'):
            from BTrees.OOBTree import OOBTree
            dbroot['site_db'] = OOBTree()
            self.site_db = dbroot['site_db']
            self.site_db[scheduler.name()] = Site(scheduler.name())
        
        self.site_db = dbroot['site_db']
      

    def add(self,job):
        self.job_db[job.id] = job
        transaction.commit()
        
    def get(self,job_id):
        return self.job_db[job_id]
    
    def list(self):
        pass
    
    def commit(self):
        transaction.commit()
            
    def close(self):
        self.connection.close()
        self.db.close()
        self.storage.close()

class Site(Persistent):
    def __init__(self,name):
        self.name = name
        self.PUE = 0.0
        self.total_num_nodes = 0
        self.total_storage_power = 0.0
        self.total_network_power = 0.0
        self.total_management_power = 0.0
        
class Queue(Persistent):
    def __init__(self,name,site_name):
        self.name = name
        self.site_name = site_name
        self.node_power = 0.0
        self.node_charge = 0.0
        
class User(Persistent):
    def __init__(self, first_name, last_name, email):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        
    def update_email(self,email):
        self.email = email
        transaction.commit()
    def firstname(self,name):
        self.first_name = name
        transaction.commit()
    def lastname(self,name):
        self.last_name = name
        transaction.commit()

class Job(Persistent):
    def __init__(self, job_id, time_stamp, scheduler):
        self.id     = job_id
        self.time_stamp = time_stamp
        self.scheduler  = scheduler
        self.status     = 'submitted'
        self.num_tasks = 0
        self.tasks_per_node = 0
        self.threads_per_task = 0
        self.num_nodes = 0
        self.stats = {}
        self.sysscribe = None
        self.app_name = 'unknown'
        self.app_data_metric = 0
        
    def update_status(self,new_status):
        self.status     = new_status
        transaction.commit()
        
    def update_sysscribe(self,sys_dict):
        self.sysscribe = sys_dict
        self._p_changed = True
        transaction.commit()
        
    def update_stats(self,stats):
        self.stats = stats
        self._p_changed = True
        transaction.commit()
        
    def appname(self,name):
        self.app_name = name
        transaction.commit()
    def appdata(self,data):
        self.app_data_metric = data
        transaction.commit()