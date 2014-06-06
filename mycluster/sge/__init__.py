import os
import re

""""
SGE notes

list PARALLEL_ENV: qconf -spl
details: qconf -sp $PARALLEL_ENV

List avail resources: qstat -pe $PARALLEL_ENV -g c

submit: qsub -pe $PARALLEL_ENV $NUM_SLOTS

delete: qdel job-id

checks: qalter -w p job-id
        qalter -w v job-id
        
        qconf -shgrp_resolved @el6nodes
        
list hosts qhost -q
        
Useful sites:
https://confluence.rcs.griffith.edu.au/display/v20zCluster/SGE+cheat+sheet
http://www.uibk.ac.at/zid/systeme/hpc-systeme/common/tutorials/sge-howto.html

"""

def queues():
    
    # list all parallel env
    # for parallel_env list queues associated
    # Find first node with queue and record node config
    
    queue_list = []
    parallel_env_list = []
    
    with os.popen('qconf -spl') as f:
        for line in f:
            parallel_env_list.append(line.strip())

    for parallel_env in parallel_env_list:
        with os.popen('qstat -pe '+parallel_env+' -g c') as f:
            f.readline(); # read header
            f.readline(); # read separator
            for line in f:
                queue_name = line.split(' ')[0]
                queue_list.append(parallel_env+':'+queue_name)
    
    return queue_list

def available_tasks(queue_id):
    
    # split queue id into queue and parallel env
    # list free slots
    free_slots = 0
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    with os.popen(' qstat -pe '+parallel_env+' -g c') as f:
        f.readline(); # read header
        f.readline(); # read separator
        for line in f:
            # remove multiple white space
            new_line = re.sub(' +',' ',line)
            qn = new_line.split(' ')[0]
            if qn == queue_name:
                free_slots = int(new_line.split(' ')[4])
                
    return free_slots

def tasks_per_node(queue_id):
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    tasks=0
    with os.popen('qconf -sq '+queue_name) as f:
        for line in f:
            if line.split(' ')[0] == 'slots':
                new_line = re.sub(' +',' ',line)
                tasks = int(new_line.split(' ')[1])
    return tasks

def node_config(queue_id):
    # Find first node with queue and record node config
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    host_group=0
    with os.popen('qconf -sq '+queue_name) as f:
        for line in f:
            if line.split(' ')[0] == 'hostlist':
                new_line = re.sub(' +',' ',line)
                host_group = int(new_line.split(' ')[1])
    
    host_name=''
    with os.popen('qconf -shgrp_resolved '+host_group) as f:
        for line in f:
            host_name = line.split(' ')[0]
            break

    config = {}

    with os.popen('qhost -q -h '+host_name) as f:
        f.readline(); # read header
        f.readline(); # read separator
        for line in f:
            if line[0] != ' ':
                name = line.split(' ')[0]
                if name != 'global':
                    new_line = re.sub(' +',' ',line)
                    config['max task']   = int(new_line.split(' ')[4])
                    config['max thread'] = int(new_line.split(' ')[5])
                    config['max memory'] =     new_line.split(' ')[7]
                
    return config

def submit(queue_id):
    pass

def delete(job_id):
    pass

def status(job_id=None):
    pass