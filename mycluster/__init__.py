import os
import time

JOB_SCHEDULERS = ('SGE','SLURM','LSF','PBS','TORQUE','MAUI','LOADLEVELER')

scheduler = None 
job_db = None

def detect_scheduling_sys():

    if os.getenv('SGE_CLUSTER_NAME') != None:
        return my_import('mycluster.sge')
    
    if os.getenv('SLURMHOME') != None:
        return my_import('mycluster.slurm')
    
    return None

def queues():
    return scheduler.queues()

def create_submit(queue_id,script_name=None,**kwargs):
    
    script = scheduler.create_submit(queue_id,**kwargs)
    
    if script_name != None:
        with open(script_name,'w') as f:
            f.write(script)
    
    return script

def submit(script_name):
    job_id = scheduler.submit(script_name)
    if job_db != None:
        from mycluster.persist import Job
        job = Job(job_id,time.time(),'sge')
        job_db.add(job)
        
    return job_id

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def get_directory():
    from os.path import expanduser
    home = expanduser("~")
    directory = home+'.mycluster'
    return directory

def create_directory():
    directory = get_directory()
    if not os.path.exists(directory):
        os.makedirs(directory)

def create_db():
    global job_db
    try:
        from mycluster.persist import JobDB
        job_db = JobDB()    
    except:
        pass
        
def init():
    global scheduler
    create_directory()
    create_db()
    scheduler = detect_scheduling_sys()
"""
Module initialiser functions
"""
init()

if __name__ == "__main__":
    queues()