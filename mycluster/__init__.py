import os
import time

VERSION = (0, 1, 0)
__version__ = '.'.join(map(str, VERSION)) + '-dev'


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

def print_queue_info():
    print('{0:25} | {1:^10} | {2:^10} | {3:^10}'.format('Queue Name','Max Task','Max Thread','Max Memory'))
    for q in queues():
        nc = scheduler.node_config(q)
        tpn = scheduler.tasks_per_node(q)
        print('{0:25} | {1:^10} | {2:^10} | {3:^10}'.format(q, tpn, nc['max thread'], nc['max memory']))

def create_submit(queue_id,script_name=None,**kwargs):

    if job_db != None:
        if 'user_email' not in kwargs:
            email = job_db.user_db['user'].email
            if email != 'unknown':
                kwargs['user_email'] = email
    
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
        with open(script_name,'r') as f:
            for line in f:
                if line.split('=')[0] == 'export NUM_TASKS':
                    job.num_tasks = line.split('=')[1]
                if line.split('=')[0] == 'export TASKS_PER_NODE':
                    job.tasks_per_node = line.split('=')[1]
                if line.split('=')[0] == 'export THREADS_PER_TASK':
                    job.theads_per_task = line.split('=')[1]
                if line.split('=')[0] == 'export NUM_NODES':
                    job.num_nodes = line.split('=')[1]
                    
        job_db.add(job)
        
    return job_id

def delete(job_id):
    scheduler.delete(job_id)

def job_list():
    if job_db != None:
        return job_db.job_db
    return None
    
def get_job(job_id):
    if job_db != None:
        return job_db.get(job_id)
    return None

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def get_directory():
    from os.path import expanduser
    home = expanduser("~")
    directory = home+'/.mycluster/'
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
        
def update_db():
    status_dict = scheduler.status()
    print status_dict
    jobs = job_list()
    for j in jobs:
        if jobs[j].status != 'completed':
            if j in status_dict:
                state = status_dict[j]
                if state == 'r':
                    jobs[j].update_status('running')
            else:
                jobs[j].update_status('completed')
                jobs[j].stats = scheduler.job_stats(j)
                
def sysscribe_update(job_id):
    if job_db != None:
        from sysscribe import system
        job_db.get(job_id).sysscribe = system.system_dict()

def email_update(email):
    if job_db != None:
        job_db.user_db['user'].update_email(email)

def init():
    global scheduler
    scheduler = detect_scheduling_sys()
    create_directory()
    create_db()
    update_db()
    
    
"""
Module initialiser functions
"""
init()

if __name__ == "__main__":
    queues()