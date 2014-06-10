import ZODB
from persistent import Persistent
from ZODB import FileStorage, DB
from mycluster import get_directory
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

    def add(self,job):
        self.job_db[job.id] = job
        transaction.commit()
        
    def get(self,job_id):
        return self.job_db[job_id]
            
    def close(self):
        self.connection.close()
        self.db.close()
        self.storage.close()

class Job(Persistent):
    def __init__(self, job_id, time_stamp, scheduler):
        self.id     = job_id
        self.time_stamp = time_stamp
        self.scheduler  = scheduler
        self.status     = 'submitted'
        
    def update_status(self,new_status):
        self.status     = new_status
        self._p_changed = 1
        