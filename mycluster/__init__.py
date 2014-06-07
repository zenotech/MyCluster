import os


JOB_SCHEDULERS = ('SGE','SLURM','LSF','PBS','TORQUE','MAUI','LOADLEVELER')

scheduler = None 

def detect_scheduling_sys():

    if os.getenv('SGE_CLUSTER_NAME') != None:
        return __import__('sge')
    
    if os.getenv('SLURMHOME') != None:
        return __import__('slurm')
    
    return None

def queues():
    return scheduler.queues()

def create_submit(queue_id,**kwargs):
    
    script = scheduler.create_submit(queue_id,kwargs)
    
    return script

"""
Module initialiser functions
"""
scheduler = detect_scheduling_sys()

if __name__ == "__main__":
    queues()