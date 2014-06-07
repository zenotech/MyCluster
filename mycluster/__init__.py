import os


JOB_SCHEDULERS = ('SGE','SLURM','LSF','PBS','TORQUE','MAUI','LOADLEVELER')

scheduler = None 

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
    return scheduler.submit(script_name)

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

"""
Module initialiser functions
"""
scheduler = detect_scheduling_sys()

if __name__ == "__main__":
    queues()