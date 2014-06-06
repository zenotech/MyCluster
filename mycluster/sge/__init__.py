import os

""""
SGE notes

list PARALLEL_ENV: qconf -spl
details: qconf -sp $PARALLEL_ENV

List avail resources: qstat -pe $PARALLEL_ENV -g c

submit: qsub -pe $PARALLEL_ENV $NUM_SLOTS

delete: qdel job-id

checks: qalter -w p job-id
        qalter -w v job-id
        
list hosts qhost -q
        
Useful sites:
https://confluence.rcs.griffith.edu.au/display/v20zCluster/SGE+cheat+sheet
http://www.uibk.ac.at/zid/systeme/hpc-systeme/common/tutorials/sge-howto.html

"""

def list_queue():
    
    # list all parallel env
    # for parallel_env list queues associated
    # Find first node with queue and record node config
    
    queue_list = []
    parallel_env_list = []
    
    with os.popen('qconf -spl') as f:
        for line in f:
            parallel_env_list.append(line.strip())

    for parallel_env in parallel_env_list:
        with os.popen(' qstat -pe '+parallel_env+' -g c') as f:
            f.readline(); # read header
            f.readline(); # read separator
            for line in f:
                queue_name = line.split(' ')[0]
                queue_list.append(parallel_env+':'+queue_name)
    
    return queue_list

def list_free_slots(queue_id):
    
    # split queue id into queue and parallel env
    # list free slots
    free_slots = 0
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    with os.popen(' qstat -pe '+parallel_env+' -g c') as f:
        f.readline(); # read header
        f.readline(); # read separator
        for line in f:
            qn = line.split(' ')[0]
            if qn == queue_name:
                free_slots = line.split(' ')[4]
                
    return free_slots

def list_node_config(queue_id):
    # Find first node with queue and record node config
    pass

def submit(queue_id):
    pass

def delete(job_id):
    pass

def status(job_id=None):
    pass