
from builtins import str
import math
import os
from string import Template

from .mycluster import get_data
from .mycluster import load_template
from .mycluster import get_timedelta
from .mycluster import check_output


def scheduler_type():
    return 'pbs'


def name():
    return 'pbs'


def queues():
    queue_list = []
    try:
        output = check_output(['qstat', '-Q'])
        lines = output.splitlines()[2:]
        for queue in lines:
            queue_list.append(queue.split(' ')[0])
    except Exception as e:
        print("ERROR")
        print(e)
        pass
    return queue_list


def accounts():
    return []


def _get_vnode_name(queue_id):
    try:
        output = check_output(['qstat', '-Qf', queue_id])
        for line in output.splitlines():
            if line.strip().startswith("default_chunk.vntype"):
                return line.split("=")[-1].strip()
    except Exception as e:
        print("ERROR")
        print(e)
    return None


def available_tasks(queue_id):
    free_tasks = 0
    max_tasks = 0
    assigned_tasks = 0
    try:
        vnode_type = _get_vnode_name(queue_id)
        if vnode_type is not None:
            output = check_output(
                'pbsnodes -a -F dsv | grep {}'.format(vnode_type), shell=True)
        for line in output.splitlines():
            for item in line.split("|"):
                [key, value] = item.strip().split('=')
                if key.strip() == 'resources_available.ncpus':
                    max_tasks += int(value)
                elif key.strip() == 'resources_assigned.ncpus':
                    assigned_tasks += int(value)
        free_tasks = max_tasks - assigned_tasks
    except Exception as e:
        print("ERROR")
        print(e)
        pass
    return {'available': free_tasks, 'max tasks': max_tasks}


def tasks_per_node(queue_id):
    tpn = 1
    try:
        vnode_type = vnode_type = _get_vnode_name(queue_id)
        if vnode_type is not None:
            output = check_output(
                'pbsnodes -a -F dsv | grep {}'.format(vnode_type), shell=True)
            for line in output.splitlines():
                for item in line.split("|"):
                    [key, value] = item.strip().split('=')
                    if key.strip() == 'resources_available.ncpus':
                        if int(value) > tpn:
                            tpn = int(value)
    except Exception as e:
        print("ERROR")
        print(e)
        pass
    return tpn


def min_tasks_per_node(queue_id):
    return 1


def node_config(queue_id):
    max_threads = 1
    max_memory = 1
    try:
        tpn = tasks_per_node(queue_id)
        vnode_type = vnode_type = vnode_type = _get_vnode_name(queue_id)
        if vnode_type is not None:
            output = check_output(
                'pbsnodes -a -F dsv | grep {}'.format(vnode_type), shell=True)
        for line in output.splitlines():
            for item in line.split("|"):
                [key, value] = item.strip().split('=')
                if key.strip() == 'resources_available.vps_per_ppu':
                    if int(value) > max_threads:
                        max_threads = int(value) * tpn
                if key.strip() == 'resources_available.mem':
                    # strip kb and convert to mb
                    mem = float(value[:-2]) / 1024
                    if mem > max_memory:
                        max_memory = mem
    except Exception as e:
        print("ERROR")
        print(e)
        pass
    return {'max thread': max_threads, 'max memory': max_memory}


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

    wall_clock = kwargs.get('wall_clock', '12:00:00')
    if ':' not in wall_clock:
        wall_clock = wall_clock + ':00:00'

    num_nodes = int(math.ceil(float(num_tasks) / float(tpn)))

    if num_nodes == 0:
        raise ValueError("Must request 1 or more nodes")

    num_queue_slots = num_nodes * queue_tpn

    if 'shared' in kwargs:
        if kwargs['shared'] and num_nodes == 1:  # Assumes fill up rule
            num_queue_slots = num_nodes * \
                max(tpn, min_tasks_per_node(queue_id))

    no_syscribe = kwargs.get('no_syscribe', False)

    record_job = not no_syscribe

    openmpi_args = kwargs.get('openmpi_args', "-bysocket -bind-to-socket")

    qos = kwargs.get('qos', None)

    template = load_template('pbs.jinja')

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
    if not immediate:
        if depends_on and depends_on_always_run:
            with os.popen('qsub -W depend=afterany:%s %s' % (depends_on, script_name)) as f:
                output = f.readline()
                try:
                    job_id = output.strip().split('.')[0]
                except:
                    print(('Job submission failed: ' + output))
        elif depends_on is not None:
            with os.popen('qsub -W depend=afterok:%s %s' % (depends_on, script_name)) as f:
                output = f.readline()
                try:
                    job_id = output.strip().split('.')[0]
                except:
                    print(('Job submission failed: ' + output))
        else:
            with os.popen('qsub ' + script_name) as f:
                output = f.readline()
                try:
                    job_id = output.strip().split('.')[0]
                except:
                    print(('Job submission failed: ' + output))
    else:
        print("immediate not yet implemented for PBS")
    return job_id


def delete(job_id):
    with os.popen('qdel ' + job_id) as f:
        pass


def status():
    status_dict = {}
    with os.popen('qstat') as f:
        pass
    return status_dict


def job_stats(job_id):
    stats_dict = {}

    return stats_dict


def job_stats_enhanced(job_id):
    """
    Get full job and step stats for job_id
    """
    stats_dict = {}
    with os.popen('qstat -xf ' + str(job_id)) as f:
        try:
            line = f.readline().strip()
            while line:
                if line.startswith('Job Id:'):
                    stats_dict['job_id'] = line.split(
                        ':')[1].split('.')[0].strip()
                elif line.startswith('resources_used.walltime'):
                    stats_dict['wallclock'] = get_timedelta(line.split('=')[1])
                elif line.startswith('resources_used.cput'):
                    stats_dict['cpu'] = get_timedelta(line.split('=')[1])
                elif line.startswith('queue'):
                    stats_dict['queue'] = line.split('=')[1].strip()
                elif line.startswith('job_state'):
                    stats_dict['status'] = line.split('=')[1].strip()
                elif line.startswith('Exit_status'):
                    stats_dict['exit_code'] = line.split('=')[1].strip()
                elif line.startswith('Exit_status'):
                    stats_dict['exit_code'] = line.split('=')[1].strip()
                elif line.startswith('stime'):
                    stats_dict['start'] = line.split('=')[1].strip()
                line = f.readline().strip()
            if stats_dict['status'] == 'F' and 'exit_code' not in stats_dict:
                stats_dict['status'] = 'CA'
            elif stats_dict['status'] == 'F' and stats_dict['exit_code'] == '0':
                stats_dict['status'] = 'PBS_F'
        except Exception as e:
            with os.popen('qstat -xaw ' + str(job_id)) as f:
                try:
                    output = f.readlines()
                    for line in output:
                        if str(job_id) in line:
                            cols = line.split()
                            stats_dict['job_id'] = cols[0].split('.')[0]
                            stats_dict['queue'] = cols[2]
                            stats_dict['status'] = cols[9]
                            stats_dict['wallclock'] = get_timedelta(cols[10])
                            return stats_dict
                except Exception as e:
                    print(e)
                    print('PBS: Error reading job stats')
                    stats_dict['status'] = 'UNKNOWN'
    return stats_dict


def running_stats(job_id):
    stats_dict = {}
    with os.popen('qstat -xaw ' + str(job_id)) as f:
        try:
            output = f.readlines()
            for line in output:
                if str(job_id) in line:
                    cols = line.split()
                    stats_dict['job_id'] = cols[0].split('.')[0]
                    stats_dict['queue'] = cols[2]
                    stats_dict['status'] = cols[9]
                    stats_dict['wallclock'] = get_timedelta(cols[10])
                    return stats_dict
        except Exception as e:
            print(e)
    return stats_dict
