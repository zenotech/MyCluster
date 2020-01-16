

from builtins import str
import os
import re
import math
from string import Template
from datetime import timedelta
from subprocess import Popen, PIPE
from .mycluster import get_data
from .mycluster import load_template
from .mycluster import check_output

"""

bjobs -u all -q emerald
bqueues -l emerald

"""


def scheduler_type():
    return 'lsf'


def name():
    lsid_output = check_output(['lsid']).splitlines()
    for line in lsid_output:
        if line.startswith('My cluster name is'):
            return line.rsplit(' ', 1)[1].strip()

    return 'undefined'


def queues():
    queue_list = []

    with os.popen('bqueues -w -u `whoami`') as f:
        f.readline()  # read header
        for line in f:
            q = line.split(' ')[0].strip()
            queue_list.append(q)

    return queue_list


def accounts():
    return []


def available_tasks(queue_id):

    # split queue id into queue and parallel env
    # list free slots
    free_tasks = 0
    max_tasks = 0
    run_tasks = 0
    queue_name = queue_id
    q_output = check_output(['bqueues', queue_name]).splitlines()
    for line in q_output:
        if line.startswith(queue_name):
            new_line = re.sub(' +', ' ', line).strip()
            try:
                max_tasks = int(new_line.split(' ')[4])
            except:
                pass
            pen_tasks = int(new_line.split(' ')[8])
            run_tasks = int(new_line.split(' ')[9])
            sus_tasks = int(new_line.split(' ')[10])

    return {'available': max_tasks - run_tasks, 'max tasks': max_tasks}


def tasks_per_node(queue_id):
    host_list = None
    q_output = check_output(['bqueues', '-l', queue_id]).splitlines()
    for line in q_output:
        if line.startswith('HOSTS:'):
            host_list = line.strip().rsplit(' ', 1)[1].replace('/', '')
            if host_list == 'none':
                return 0
    bhosts_output = check_output(['bhosts', '-l', host_list]).splitlines()
    line = re.sub(' +', ' ', bhosts_output[2]).strip()
    tasks = int(line.split(' ')[3])

    return tasks


def node_config(queue_id):
    # Find first node with queue and record node config
    # bqueues -l queue_id
    host_list = None
    config = {}
    q_output = check_output(['bqueues', '-l', queue_id]).splitlines()
    for line in q_output:
        if line.startswith('HOSTS:'):
            host_list = line.strip().rsplit(' ', 1)[1].replace('/', '')
            if host_list == 'none':
                config['max task'] = 0
                config['max thread'] = 0
                config['max memory'] = 0
                return config
    bhosts_output = check_output(['bhosts', '-l', host_list]).splitlines()
    line = re.sub(' +', ' ', bhosts_output[2]).strip()
    tasks = int(line.split(' ')[3])
    line = re.sub(' +', ' ', bhosts_output[6]).strip()
    memory = float(line.split(' ')[11].replace('G', ''))
    config['max task'] = tasks
    config['max thread'] = tasks
    config['max memory'] = memory

    return config


def create_submit(queue_id, **kwargs):
    queue_name = queue_id
    num_tasks = 1
    if 'num_tasks' in kwargs:
        num_tasks = kwargs['num_tasks']

    tpn = tasks_per_node(queue_id)
    queue_tpn = tpn

    if 'tasks_per_node' in kwargs:
        tpn = kwargs['tasks_per_node']

    nc = node_config(queue_id)
    qc = available_tasks(queue_id)

    if qc['max tasks'] > 0:
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

    wall_clock = kwargs.get('wall_clock', '12:00')
    if ':' not in wall_clock:
        wall_clock = wall_clock + ':00'

    num_nodes = int(math.ceil(float(num_tasks) / float(tpn)))

    num_queue_slots = num_nodes * queue_tpn

    no_syscribe = kwargs.get('no_syscribe', False)

    record_job = not no_syscribe

    openmpi_args = kwargs.get('openmpi_args', "-bysocket -bind-to-socket")

    qos = kwargs.get('qos', None)

    template = load_template('lsf.jinja')

    script_str = template.render(my_name=my_name,
                                 my_script=my_script,
                                 my_output=my_output,
                                 user_email=user_email,
                                 queue_name=queue_name,
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


def submit(script_name, immediate, depends_on=None,
           depends_on_always_run=False):
    job_id = None

    if depends_on and depends_on_always_run:
        cmd = 'bsub -w "ended(%s)" < %s ' % (depends_on, script_name)
        with os.popen(cmd) as f:
            output = f.readline()
            try:
                job_id = int(output.split(' ')[1].replace(
                    '<', '').replace('>', ''))
            except:
                print(('Job submission failed: ' + output))
    elif depends_on is not None:
        cmd = 'bsub -w "done(%s)" < %s ' % (depends_on, script_name)
        with os.popen(cmd) as f:
            output = f.readline()
            try:
                job_id = int(output.split(' ')[1].replace(
                    '<', '').replace('>', ''))
            except:
                print(('Job submission failed: ' + output))
    else:
        with os.popen('bsub <' + script_name) as f:
            output = f.readline()
            try:
                job_id = int(output.split(' ')[1].replace(
                    '<', '').replace('>', ''))
            except:
                print(('Job submission failed: ' + output))
    return job_id


def delete(job_id):
    with os.popen('bkill ' + job_id) as f:
        pass


def status():
    status_dict = {}
    with os.popen('bjobs -w') as f:
        try:
            f.readline()  # read header
            for line in f:
                new_line = re.sub(' +', ' ', line.strip())
                job_id = int(new_line.split(' ')[0])
                state = new_line.split(' ')[2]
                if state == 'RUN':
                    status_dict[job_id] = 'r'
                else:
                    status_dict[job_id] = state
        except e:
            print(e)

    return status_dict


def job_stats(job_id):
    stats_dict = {}
    with os.popen('bacct -l ' + str(job_id)) as f:
        try:
            line = f.readline()
            new_line = re.sub(' +', ' ', line.strip())
            stats_dict['wallclock'] = new_line.split(' ')[0]
            stats_dict['cpu'] = new_line.split(' ')[1]
            stats_dict['queue'] = new_line.split(' ')[2]
            # float(new_line.split(' ')[4])*int(new_line.split(' ')[3])
            stats_dict['mem'] = '-'
        except:
            print('LSF: Error reading job stats')

    return stats_dict


def job_stats_enhanced(job_id):
    """
    Get full job and step stats for job_id
    """
    stats_dict = {}
    with os.popen('bjobs -o "jobid run_time cpu_used  queue slots  stat exit_code start_time estimated_start_time finish_time delimiter=\'|\'" -noheader ' + str(job_id)) as f:
        try:
            line = f.readline()
            cols = line.split('|')
            stats_dict['job_id'] = cols[0]
            if cols[1] != '-':
                stats_dict['wallclock'] = timedelta(
                    seconds=float(cols[1].split(' ')[0]))
            if cols[2] != '-':
                stats_dict['cpu'] = timedelta(
                    seconds=float(cols[2].split(' ')[0]))
            stats_dict['queue'] = cols[3]
            stats_dict['status'] = cols[5]
            stats_dict['exit_code'] = cols[6]
            stats_dict['start'] = cols[7]
            stats_dict['start_time'] = cols[8]
            if stats_dict['status'] in ['DONE', 'EXIT']:
                stats_dict['end'] = cols[9]

            steps = []
            stats_dict['steps'] = steps
        except:
            with os.popen('bhist -l ' + str(job_id)) as f:
                try:
                    output = f.readlines()
                    for line in output:
                        if "Done successfully" in line:
                            stats_dict['status'] = 'DONE'
                            return stats_dict
                        elif "Completed <exit>" in line:
                            stats_dict['status'] = 'EXIT'
                            return stats_dict
                        else:
                            stats_dict['status'] = 'UNKNOWN'
                except Exception as e:
                    print(e)
                    print('LSF: Error reading job stats')
                    stats_dict['status'] = 'UNKNOWN'
    return stats_dict


def running_stats(job_id):
    stats_dict = {}
    with os.popen('bjobs -W ' + str(job_id)) as f:
        try:
            line = f.readline()
            new_line = re.sub(' +', ' ', line.strip())
            stats_dict['wallclock'] = new_line.split(' ')[0]
        except:
            pass

    with os.popen('bjobs -W ' + str(job_id)) as f:
        try:
            line = f.readline()
            new_line = re.sub(' +', ' ', line.strip())
            ntasks = int(new_line.split(' ')[2])
            stats_dict['mem'] = float(new_line.split(' ')[1]) * ntasks
            stats_dict['cpu'] = float(new_line.split(' ')[0]) * ntasks
        except:
            pass

    return stats_dict
