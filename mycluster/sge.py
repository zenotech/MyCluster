
from builtins import str
import os
import re
import math
from string import Template
from .mycluster import get_data
from .mycluster import load_template

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
        with os.popen('qstat -pe ' + parallel_env + ' -U `whoami` -g c') as f:
            f.readline()  # read header
            f.readline()  # read separator
            for line in f:
                queue_name = line.split(' ')[0].strip()
                # Check if user has permission to use queue
                with os.popen('qstat -g c -U `whoami` -q ' + queue_name) as f2:
                    try:
                        f2.readline()
                        f2.readline()
                        if len(f2.readline()):
                            queue_list.append(parallel_env + ':' + queue_name)
                    except:
                        pass

    return queue_list


def accounts():
    return []


def available_tasks(queue_id):

    # split queue id into queue and parallel env
    # list free slots
    free_tasks = 0
    max_tasks = 0
    parallel_env = queue_id.split(':')[0]
    queue_name = queue_id.split(':')[1]
    with os.popen(' qstat -pe ' + parallel_env + ' -U `whoami` -g c') as f:
        f.readline()  # read header
        f.readline()  # read separator
        for line in f:
            # remove multiple white space
            new_line = re.sub(' +', ' ', line)
            qn = new_line.split(' ')[0]
            if qn == queue_name:
                free_tasks = int(new_line.split(' ')[4])
                max_tasks = int(new_line.split(' ')[5])

    return {'available': free_tasks, 'max tasks': max_tasks}


def tasks_per_node(queue_id):
    parallel_env = queue_id.split(':')[0]
    queue_name = queue_id.split(':')[1]
    tasks = 0
    with os.popen('qconf -sq ' + queue_name) as f:
        for line in f:
            if line.split(' ')[0] == 'slots':
                tasks = int(re.split('\W+', line)[1])

    pe_tasks = tasks
    with os.popen('qconf -sp ' + parallel_env) as f:
        try:
            for line in f:
                if line.split(' ')[0] == 'allocation_rule':
                    # This may throw exception as allocation rule
                    # may not always be an integer
                    pe_tasks = int(re.split('\W+', line)[1])
        except:
            pass

    return min(tasks, pe_tasks)


def min_tasks_per_node(queue_id):
    """
    This function is used when requesting non exclusive use
    as the parallel environment might enforce a minimum number
    of tasks
    """
    parallel_env = queue_id.split(':')[0]
    queue_name = queue_id.split(':')[1]
    tasks = 1
    pe_tasks = tasks
    with os.popen('qconf -sp ' + parallel_env) as f:
        try:
            for line in f:
                if line.split(' ')[0] == 'allocation_rule':
                    # This may throw exception as allocation rule
                    # may not always be an integer
                    pe_tasks = int(re.split('\W+', line)[1])
        except:
            pass

    return max(tasks, pe_tasks)


def node_config(queue_id):
    # Find first node with queue and record node config
    parallel_env = queue_id.split(':')[0]
    queue_name = queue_id.split(':')[1]
    host_group = 0
    with os.popen('qconf -sq ' + queue_name) as f:
        for line in f:
            if line.split(' ')[0] == 'hostlist':
                new_line = re.sub(' +', ' ', line)
                host_group = new_line.split(' ')[1]

    config = {}
    host_name = ''
    found = False
    if host_group[0] is '@':
        # Is a host group
        with os.popen('qconf -shgrp_resolved ' + host_group) as f:
            for line in f:
                for host_name in line.split(' '):
                    with os.popen('qhost -q -h ' + host_name) as f:
                        header = f.readline()  # read header
                        f.readline()  # read separator
                        new_header = re.sub(' +', ' ', header).strip()
                        # sge <=6.2u4 style
                        if (new_header.split(' ')[3]) == 'LOAD':
                            for line in f:
                                if line[0] != ' ':
                                    name = line.split(' ')[0]
                                    if name != 'global':
                                        new_line = re.sub(
                                            ' +', ' ', line).strip()
                                        if new_line.split(' ')[3] != '-':
                                            config['max task'] = int(
                                                new_line.split(' ')[2])
                                            config['max thread'] = int(
                                                new_line.split(' ')[2])
                                            config['max memory'] = new_line.split(' ')[
                                                4]
                                            found = True
                                            break
                        else:
                            for line in f:
                                if line[0] != ' ':
                                    name = line.split(' ')[0]
                                    if name != 'global':
                                        new_line = re.sub(
                                            ' +', ' ', line).strip()
                                        if new_line.split(' ')[3] != '-':
                                            config['max task'] = int(
                                                new_line.split(' ')[4])
                                            config['max thread'] = int(
                                                new_line.split(' ')[5])
                                            config['max memory'] = new_line.split(' ')[
                                                7]
                                            found = True
                                            break
                    if found:
                        break
    else:
        # Is a host
        host_name = host_group
        with os.popen('qhost -q -h ' + host_name) as f:
            header = f.readline()  # read header
            f.readline()  # read separator
            new_header = re.sub(' +', ' ', header).strip()
            if (new_header.split(' ')[3]) == 'LOAD':  # sge <=6.2u4 style
                for line in f:
                    if line[0] != ' ':
                        name = line.split(' ')[0]
                        if name != 'global':
                            new_line = re.sub(' +', ' ', line).strip()
                            if new_line.split(' ')[3] != '-':
                                config['max task'] = int(
                                    new_line.split(' ')[2])
                                config['max thread'] = int(
                                    new_line.split(' ')[2])
                                config['max memory'] = new_line.split(' ')[4]
                            else:
                                config['max task'] = 0
                                config['max thread'] = 0
                                config['max memory'] = 0
            else:
                for line in f:
                    if line[0] != ' ':
                        name = line.split(' ')[0]
                        if name != 'global':
                            new_line = re.sub(' +', ' ', line).strip()
                            if new_line.split(' ')[3] != '-':
                                config['max task'] = int(
                                    new_line.split(' ')[4])
                                config['max thread'] = int(
                                    new_line.split(' ')[5])
                                config['max memory'] = new_line.split(' ')[7]
                            else:
                                config['max task'] = 0
                                config['max thread'] = 0
                                config['max memory'] = 0
    return config


def create_submit(queue_id, **kwargs):

    parallel_env = queue_id.split(':')[0]
    queue_name = queue_id.split(':')[1]

    num_tasks = 1
    if 'num_tasks' in kwargs:
        num_tasks = kwargs['num_tasks']

    tpn = tasks_per_node(queue_id)
    queue_tpn = tpn

    if 'tasks_per_node' in kwargs:
        tpn = kwargs['tasks_per_node']

    nc = node_config(queue_id)
    qc = available_tasks(queue_id)

    num_tasks = min(num_tasks, qc['max tasks'])

    num_threads_per_task = nc['max thread']
    if 'num_threads_per_task' in kwargs:
        num_threads_per_task = kwargs['num_threads_per_task']
    num_threads_per_task = min(num_threads_per_task, int(
        math.ceil(float(nc['max thread']) / float(tpn))))

    my_name = kwargs.get('my_name', "myclusterjob")
    my_output = kwargs.get('my_output', "myclusterjob.out")
    my_script = kwargs.get('my_script', None)

    if 'mycluster-' in my_script:
        my_script = get_data(my_script)

    user_email = kwargs.get('user_email', None)
    project_name = kwargs.get('project_name', 'default')

    wall_clock = kwargs.get('wall_clock', '12:00:00')
    if ':' not in wall_clock:
        wall_clock = wall_clock + ':00:00'

    num_nodes = int(math.ceil(float(num_tasks) / float(tpn)))

    # For exclusive node use total number of slots required
    # is number of nodes x number of slots offer by queue
    num_queue_slots = num_nodes * queue_tpn
    if 'shared' in kwargs:
        if kwargs['shared'] and num_nodes == 1:  # Assumes fill up rule
            num_queue_slots = num_nodes * \
                max(tpn, min_tasks_per_node(queue_id))

    no_syscribe = kwargs.get('no_syscribe', False)

    record_job = not no_syscribe

    openmpi_args = kwargs.get('openmpi_args', "-bysocket -bind-to-socket")

    qos = kwargs.get('qos', None)

    template = load_template('sge.jinja')

    script_str = template.render(my_name=my_name,
                                 my_script=my_script,
                                 my_output=my_output,
                                 user_email=user_email,
                                 queue_name=queue_name,
                                 parallel_env=parallel_env,
                                 num_queue_slots=num_queue_slots,
                                 num_tasks=num_tasks,
                                 tpn=tpn,
                                 num_threads_per_task=num_threads_per_task,
                                 num_nodes=num_nodes,
                                 project_name=project_name,
                                 wall_clock=wall_clock,
                                 record_job=record_job,
                                 openmpi_args=openmpi_args,
                                 qos=qos)

    return script_str


def submit(script_name, immediate, depends=None):
    job_id = None
    with os.popen('qsub -V -terse ' + script_name) as f:
        job_id = 0
        try:
            job_id = int(f.readline().strip())
        except:
            print('job id not returned')
            print((f.readline()))
            pass
        # Get job id and record in database
    return job_id


def delete(job_id):
    with os.popen('qdel ' + job_id) as f:
        pass


def status():
    status_dict = {}
    with os.popen('qstat') as f:
        try:
            f.readline()  # read header
            f.readline()  # read separator
            for line in f:
                new_line = re.sub(' +', ' ', line.strip())
                job_id = int(new_line.split(' ')[0])
                state = new_line.split(' ')[4]

                status_dict[job_id] = state
        except e:
            print(e)

    return status_dict


def job_stats(job_id):
    stats_dict = {}
    output = {}
    with os.popen('qacct -j ' + str(job_id)) as f:
        try:
            f.readline()  # read header
            for line in f:
                new_line = re.sub(' +', ' ', line.strip())
                output[new_line.split(' ')[0]] = new_line.split(' ', 1)[1]
        except:
            pass
    import datetime
    from .mycluster import print_timedelta
    stats_dict['wallclock'] = datetime.timedelta(
        seconds=int(output['ru_wallclock']))
    stats_dict['mem'] = output['mem']
    stats_dict['cpu'] = datetime.timedelta(
        seconds=int(output['cpu'].split('.')[0]))
    stats_dict['queue'] = output['granted_pe'] + ':' + output['qname']

    return stats_dict


def running_stats(job_id):
    stats_dict = {}
    output = {}
    with os.popen('qstat -j ' + str(job_id)) as f:
        try:
            f.readline()  # read header
            for line in f:
                new_line = re.sub(' +', ' ', line.strip())
                if new_line.split(' ')[0] == 'usage':
                    mstr = new_line.split(' ', 2)[2]
                    # Note this needs to be in timedelta format
                    output['cpu'] = mstr.split(',')[0].split('=')[1]
                    output['mem'] = mstr.split(',')[1].split('=')[1]
        except:
            pass

    stats_dict['wallclock'] = 0
    stats_dict['mem'] = output['mem']
    stats_dict['cpu'] = output['cpu']

    return stats_dict
