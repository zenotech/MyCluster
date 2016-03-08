import os
import re
import math
from string import Template

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
http://www-zeuthen.desy.de/dv/documentation/unixguide/chapter18.html
http://psg.skinforum.org/lsf.html
"""

def scheduler_type():
    return 'sge'

def name():
    return os.getenv('SGE_CLUSTER_NAME')

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
        with os.popen('qstat -pe '+parallel_env+' -U `whoami` -g c') as f:
            f.readline(); # read header
            f.readline(); # read separator
            for line in f:
                queue_name = line.split(' ')[0].strip()
                # Check if user has permission to use queue
                with os.popen('qstat -g c -U `whoami` -q '+queue_name) as f2:
                    try:
                        f2.readline()
                        f2.readline()
                        if len(f2.readline()):
                            queue_list.append(parallel_env+':'+queue_name)
                    except:
                        pass
    
    return queue_list

def available_tasks(queue_id):
    
    # split queue id into queue and parallel env
    # list free slots
    free_tasks = 0
    max_tasks = 0
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    with os.popen(' qstat -pe '+parallel_env+' -U `whoami` -g c') as f:
        f.readline(); # read header
        f.readline(); # read separator
        for line in f:
            # remove multiple white space
            new_line = re.sub(' +',' ',line)
            qn = new_line.split(' ')[0]
            if qn == queue_name:
                free_tasks = int(new_line.split(' ')[4])
                max_tasks = int(new_line.split(' ')[5])
                
    return {'available' : free_tasks, 'max tasks' : max_tasks}

def tasks_per_node(queue_id):
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    tasks=0
    with os.popen('qconf -sq '+queue_name) as f:
        for line in f:
            if line.split(' ')[0] == 'slots':
                tasks = int(re.split('\W+', line)[1])

    pe_tasks = tasks
    with os.popen('qconf -sp '+parallel_env) as f:
        try:
            for line in f:
                if line.split(' ')[0] == 'allocation_rule':
                    # This may throw exception as allocation rule
                    # may not always be an integer
                    pe_tasks = int(re.split('\W+', line)[1])
        except:
            pass

    return min(tasks,pe_tasks)

def min_tasks_per_node(queue_id):
    """ 
    This function is used when requesting non exclusive use
    as the parallel environment might enforce a minimum number
    of tasks
    """
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    tasks=1
    pe_tasks = tasks
    with os.popen('qconf -sp '+parallel_env) as f:
        try:
            for line in f:
                if line.split(' ')[0] == 'allocation_rule':
                    # This may throw exception as allocation rule
                    # may not always be an integer
                    pe_tasks = int(re.split('\W+', line)[1])
        except:
            pass

    return max(tasks,pe_tasks)

def node_config(queue_id):
    # Find first node with queue and record node config
    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    host_group=0
    with os.popen('qconf -sq '+queue_name) as f:
        for line in f:
            if line.split(' ')[0] == 'hostlist':
                new_line = re.sub(' +',' ',line)
                host_group = new_line.split(' ')[1]
    
    config = {}
    host_name = ''
    found = False
    if host_group[0] is '@':
        #Is a host group
        with os.popen('qconf -shgrp_resolved '+host_group) as f:
            for line in f:
                for host_name in line.split(' '):
                    with os.popen('qhost -q -h '+host_name) as f:
                        header = f.readline(); # read header
                        f.readline(); # read separator
                        new_header = re.sub(' +',' ',header).strip()
                        if (new_header.split(' ')[3]) == 'LOAD': #sge <=6.2u4 style
                            for line in f:
                                if line[0] != ' ':
                                    name = line.split(' ')[0]
                                    if name != 'global':
                                        new_line = re.sub(' +',' ',line).strip()
                                        if new_line.split(' ')[3] != '-':
                                            config['max task']   = int(new_line.split(' ')[2])
                                            config['max thread'] = int(new_line.split(' ')[2])
                                            config['max memory'] =     new_line.split(' ')[4]
                                            found = True
                                            break
                        else:
                            for line in f:
                                if line[0] != ' ':
                                    name = line.split(' ')[0]
                                    if name != 'global':
                                        new_line = re.sub(' +',' ',line).strip()
                                        if new_line.split(' ')[3] != '-':
                                            config['max task']   = int(new_line.split(' ')[4])
                                            config['max thread'] = int(new_line.split(' ')[5])
                                            config['max memory'] =     new_line.split(' ')[7]
                                            found = True
                                            break
                    if found: break
    else:
        #Is a host
        host_name = host_group
        with os.popen('qhost -q -h '+host_name) as f:
            header = f.readline(); # read header
            f.readline(); # read separator
            new_header = re.sub(' +',' ',header).strip()
            if (new_header.split(' ')[3]) == 'LOAD': #sge <=6.2u4 style
                for line in f:
                    if line[0] != ' ':
                        name = line.split(' ')[0]
                        if name != 'global':
                            new_line = re.sub(' +',' ',line).strip()
                            if new_line.split(' ')[3] != '-':
                                config['max task']   = int(new_line.split(' ')[2])
                                config['max thread'] = int(new_line.split(' ')[2])
                                config['max memory'] =     new_line.split(' ')[4]
                            else:
                                config['max task']   = 0
                                config['max thread'] = 0
                                config['max memory'] = 0
            else:
                for line in f:
                    if line[0] != ' ':
                        name = line.split(' ')[0]
                        if name != 'global':
                            new_line = re.sub(' +',' ',line).strip()
                            if new_line.split(' ')[3] != '-':
                                config['max task']   = int(new_line.split(' ')[4])
                                config['max thread'] = int(new_line.split(' ')[5])
                                config['max memory'] =     new_line.split(' ')[7]  
                            else:
                                config['max task']   = 0
                                config['max thread'] = 0
                                config['max memory'] = 0              
    return config

def create_submit(queue_id,**kwargs):

    parallel_env = queue_id.split(':')[0]
    queue_name   = queue_id.split(':')[1]
    
    num_tasks = 1
    if 'num_tasks' in kwargs:
        num_tasks = kwargs['num_tasks']
    
    tpn = tasks_per_node(queue_id)
    queue_tpn = tpn
    if 'tasks_per_node' in kwargs:
        tpn = min(tpn,kwargs['tasks_per_node'])
    
    nc = node_config(queue_id)
    qc = available_tasks(queue_id)
    
    num_tasks = min(num_tasks,qc['max tasks'])
    
    num_threads_per_task = nc['max thread']
    if 'num_threads_per_task' in kwargs:
        num_threads_per_task = kwargs['num_threads_per_task']
    num_threads_per_task = min(num_threads_per_task,int(math.ceil(float(nc['max thread'])/float(tpn))))
    
    my_name = "myclusterjob"
    if 'my_name' in kwargs:
        my_name = kwargs['my_name']
    my_output = "myclusterjob.out"
    if 'my_output' in kwargs:
        my_output = kwargs['my_output']
    if 'my_script' not in kwargs:
        pass
    my_script = kwargs['my_script']
    if 'user_email' not in kwargs:
        pass
    user_email = kwargs['user_email']
    
    project_name = 'default'
    if 'project_name' in kwargs:
        project_name = kwargs['project_name']
    
    wall_clock = '12:00:00'
    if 'wall_clock' in kwargs:
        if ':' not in str(kwargs['wall_clock']):
            wall_clock = str(kwargs['wall_clock'])+':00:00'
        else:
            wall_clock = str(kwargs['wall_clock'])

    num_nodes = int(math.ceil(float(num_tasks)/float(tpn)))

    # For exclusive node use total number of slots required
    # is number of nodes x number of slots offer by queue
    num_queue_slots = num_nodes*queue_tpn
    if 'shared' in kwargs:
        if kwargs['shared'] and num_nodes == 1: # Assumes fill up rule
            num_queue_slots = num_nodes*max(tpn,min_tasks_per_node(queue_id))
    
    record_job = "True"
    if 'no_syscribe' in kwargs:
        record_job = ""

    if 'openmpi_args' not in kwargs:
        openmpi_args = "-bysocket -bind-to-socket"
    else:
        openmpi_args =  kwargs['openmpi_args']

    script=Template(r"""#!/bin/bash
#
# SGE job submission script generated by MyCluster 
#
# Job name
#$$ -N $my_name
# The batch system should use the current directory as working directory.
#$$ -cwd
# Redirect output stream to this file.
#$$ -o $my_output.$$JOB_ID
# Join the error stream to the output stream.
#$$ -j yes
# Send status information to this email address. 
#$$ -M $user_email
# Send me an e-mail when the job has finished. 
#$$ -m be
# Queue name
#$$ -q $queue_name
# Parallel environment
#$$ -pe $parallel_env $num_queue_slots
# Project name
#$$ -P $project_name
# Maximum wall clock
#$$ -l h_rt=$wall_clock

export MYCLUSTER_QUEUE=$parallel_env:$queue_name
export MYCLUSTER_JOB_NAME=$my_name
export NUM_TASKS=$num_tasks
export TASKS_PER_NODE=$tpn
export THREADS_PER_TASK=$num_threads_per_task
export NUM_NODES=$num_nodes

# OpenMP configuration
export OMP_NUM_THREADS=$$THREADS_PER_TASK
export OMP_PROC_BIND=true
export OMP_PLACES=sockets

# OpenMPI
export OMPI_CMD="mpiexec -n $$NUM_TASKS -npernode $$TASKS_PER_NODE $openmpi_args" 

# MVAPICH2
export MV2_CPU_BINDING_LEVEL=SOCKET
export MV2_CPU_BINDING_POLICY=scatter
export MVAPICH_CMD="mpiexec -n $$NUM_TASKS -ppn $$TASKS_PER_NODE -bind-to-socket"

# Intel MPI
# The following variables define a sensible pinning strategy for Intel MPI tasks -
# this should be suitable for both pure MPI and hybrid MPI/OpenMP jobs:
export I_MPI_PIN_DOMAIN=omp:compact # Domains are $$OMP_NUM_THREADS cores in size
export I_MPI_PIN_ORDER=scatter # Adjacent domains have minimal sharing of caches/sockets
#export I_MPI_FABRICS=shm:ofa
export IMPI_CMD="mpiexec -n $$NUM_TASKS -ppn $$TASKS_PER_NODE"

# Summarise environment
echo -e "JobID: $$JOB_ID\n======"
echo "Time: `date`"
echo "Running on master node: `hostname`"
echo "Current directory: `pwd`"

if [ "$$PE_HOSTFILE" ]; then
        #! Create a machine file:
        cat $$PE_HOSTFILE | awk '{print $$1, " slots=" $$2}' > machine.file.$$JOB_ID
        echo -e "\nNodes allocated:\n================"
        echo `cat machine.file.$$JOB_ID | sed -e 's/\..*$$//g'`
fi

echo -e "\nnumtasks=$num_tasks, numnodes=$num_nodes, tasks_per_node=$tpn (OMP_NUM_THREADS=$$OMP_NUM_THREADS)"

echo -e "\nExecuting command:\n==================\n$my_script\n"

# Run user script
. $my_script

# Report on completion
echo -e "\nJob Complete:\n==================\n"
if [ $record_job ]; then
    echo -e "\nRecording hardware setup\n==================\n"
    mycluster --sysscribe $$JOBID
    if [ "$$MYCLUSTER_APP_NAME" ]; then
        mycluster --jobid $$JOBID --appname=$$MYCLUSTER_APP_NAME
    fi
    if [ "$$MYCLUSTER_APP_DATA" ]; then
        mycluster --jobid $$JOBID --appdata=$$MYCLUSTER_APP_DATA
    fi
fi
echo -e "Complete========\n"
""")
    script_str = script.substitute({'my_name':my_name,
                                   'my_script':my_script,
                                   'my_output':my_output,
                                   'user_email':user_email,
                                   'queue_name':queue_name,
                                   'parallel_env':parallel_env,
                                   'num_queue_slots':num_queue_slots,
                                   'num_tasks':num_tasks,
                                   'tpn':tpn,
                                   'num_threads_per_task':num_threads_per_task,
                                   'num_queue_slots':num_queue_slots,
                                   'num_nodes':num_nodes,
                                   'project_name': project_name,
                                   'wall_clock' : wall_clock,
                                   'openmpi_args': openmpi_args,
                                   })
    
    return script_str

def submit(script_name, immediate):
    job_id = None
    with os.popen('qsub -V -terse '+script_name) as f:
        job_id = int(f.readline().strip())
        # Get job id and record in database
    return job_id

def delete(job_id):
    with os.popen('qdel '+job_id) as f:
        pass
    
def status():
    status_dict = {}
    with os.popen('qstat') as f:
        try:
            f.readline(); # read header
            f.readline(); # read separator
            for line in f:
                new_line = re.sub(' +',' ',line.strip())
                job_id = int(new_line.split(' ')[0])
                state = new_line.split(' ')[4]
                
                status_dict[job_id] = state
        except e:
            print e
        
    return status_dict
    
def job_stats(job_id):
    stats_dict = {}
    output={}
    with os.popen('qacct -j '+str(job_id)) as f:
        try:
            f.readline(); # read header
            for line in f:
                new_line = re.sub(' +',' ',line.strip())
                output[new_line.split(' ')[0]] = new_line.split(' ',1)[1]
        except:
            pass
    import datetime
    from mycluster import print_timedelta
    stats_dict['wallclock'] = datetime.timedelta(seconds=int(output['ru_wallclock']))
    stats_dict['mem'] = output['mem']
    stats_dict['cpu'] = datetime.timedelta(seconds=int(output['cpu'].split('.')[0]))
    stats_dict['queue'] = output['granted_pe']+':'+output['qname']
    
    return stats_dict

def running_stats(job_id):
    stats_dict = {}
    output={}
    with os.popen('qstat -j '+str(job_id)) as f:
        try:
            f.readline(); # read header
            for line in f:
                new_line = re.sub(' +',' ',line.strip())
                if new_line.split(' ')[0] == 'usage':
                    mstr = new_line.split(' ',2)[2]                    
                    output['cpu'] = mstr.split(',')[0].split('=')[1] # Note this needs to be in timedelta format
                    output['mem'] = mstr.split(',')[1].split('=')[1]
        except:
            pass
    
    stats_dict['wallclock'] = 0
    stats_dict['mem'] = output['mem']
    stats_dict['cpu'] = output['cpu']
    
    return stats_dict
