

from builtins import str
import os
import re
import math
from string import Template
# from datetime import timedelta
from .mycluster import check_output
from .mycluster import get_timedelta
from .mycluster import get_data
from .mycluster import load_template

from jinja2 import Environment, FileSystemLoader


"""
sacctmgr show cluster
"""


def scheduler_type():
    return 'slurm'


def name():
    with os.popen('sacctmgr show cluster') as f:
        f.readline()
        f.readline()
        return f.readline().strip().split(' ')[0]


def accounts():
    account_list = []

    with os.popen('sacctmgr --noheader list assoc user=`id -un` format=Account') as f:
        for line in f:
            account_list.append(line)

    return account_list


def queues():
    queue_list = []

    with os.popen('sinfo -sh') as f:
        for line in f:
            q = line.split(' ')[0].strip().replace("*", "")
            queue_list.append(q)

    return queue_list


def available_tasks(queue_id):

    # split queue id into queue and parallel env
    # list free slots
    free_tasks = 0
    max_tasks = 0
    queue_name = queue_id
    nc = node_config(queue_id)
    with os.popen('sinfo -sh -p ' + queue_name) as f:
        line = f.readline()
        new_line = re.sub(' +', ' ', line.strip())
        line = new_line.split(' ')[3]
        free_tasks = int(line.split('/')[1]) * nc['max task']
        max_tasks = int(line.split('/')[3]) * nc['max task']

    return {'available': free_tasks, 'max tasks': max_tasks}


def tasks_per_node(queue_id):
    queue_name = queue_id
    tasks = 0
    with os.popen('sinfo -Nelh -p ' + queue_name) as f:
        line = f.readline()
        new_line = re.sub(' +', ' ', line.strip())
        tasks = int(new_line.split(' ')[4])
    return tasks


def node_config(queue_id):
    # Find first node with queue and record node config
    queue_name = queue_id
    tasks = 0
    config = {}
    with os.popen('sinfo -Nelh -p ' + queue_name) as f:
        line = f.readline()
        if len(line):
            new_line = re.sub(' +', ' ', line.strip())
            tasks = int(new_line.split(' ')[4])
            memory = int(new_line.split(' ')[6])
            config['max task'] = tasks
            config['max thread'] = tasks
            config['max memory'] = memory
        else:
            raise Exception(
                "Requested partition %s has no nodes" % queue_name)

    return config


def create_submit(queue_id, **kwargs):

    queue_name = queue_id

    num_tasks = 1
    if 'num_tasks' in kwargs:
        num_tasks = kwargs['num_tasks']
    else:
        raise ValueError("num_tasks must be specified")

    if 'tasks_per_node' in kwargs:
        tpn = kwargs['tasks_per_node']
    else:
        tpn = tasks_per_node(queue_id)
    if 'num_threads_per_task' in kwargs:
        num_threads_per_task = kwargs['num_threads_per_task']
    else:
        raise ValueError("num_threads_per_task must be specified")

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

    no_syscribe = kwargs.get('no_syscribe', False)

    record_job = not no_syscribe

    openmpi_args = kwargs.get('openmpi_args', "-bysocket -bind-to-socket")

    qos = kwargs.get('qos', None)

    template = load_template('slurm.jinja')

    script_str = template.render(my_name=my_name,
                                 my_script=my_script,
                                 my_output=my_output,
                                 user_email=user_email,
                                 queue_name=queue_name,
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

    # Enable external specification of additional options
    additional_cmd = ''
    if 'MYCLUSTER_SUBMIT_OPT' in os.environ:
        additional_cmd = os.environ['MYCLUSTER_SUBMIT_OPT']

    if not immediate:
        if depends_on and depends_on_always_run:
            with os.popen('sbatch %s --kill-on-invalid-dep=yes --dependency=afterany:%s %s' % (additional_cmd, depends_on, script_name)) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(' ')[-1].strip())
                except:
                    print(('Job submission failed: ' + output))
        elif depends_on is not None:
            with os.popen('sbatch %s --kill-on-invalid-dep=yes --dependency=afterok:%s %s' % (additional_cmd, depends_on, script_name)) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(' ')[-1].strip())
                except:
                    print(('Job submission failed: ' + output))
        else:
            with os.popen('sbatch %s %s' % (additional_cmd, script_name)) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(' ')[-1].strip())
                except:
                    print(('Job submission failed: ' + output))
                # Get job id and record in database
    else:
        with os.popen('grep -- "SBATCH -p" ' + script_name + ' | sed \'s/#SBATCH//\'') as f:
            partition = f.readline().rstrip()
        with os.popen('grep -- "SBATCH --nodes" ' + script_name + ' | sed \'s/#SBATCH//\'') as f:
            nnodes = f.readline().rstrip()
        with os.popen('grep -- "SBATCH --ntasks" ' + script_name + ' | sed \'s/#SBATCH//\'') as f:
            ntasks = f.readline().rstrip()
        with os.popen('grep -- "SBATCH -A" ' + script_name + ' | sed \'s/#SBATCH//\'') as f:
            project = f.readline().rstrip()
        with os.popen('grep -- "SBATCH -J" ' + script_name + ' | sed \'s/#SBATCH//\'') as f:
            job = f.readline().rstrip()

        cmd_line = 'salloc --exclusive ' + nnodes + ' ' + partition + ' ' + \
            ntasks + ' ' + project + ' ' + job + ' bash ./' + script_name
        print(cmd_line)

        try:
            output = check_output(cmd_line, shell=True)
            try:
                job_id = int(output.split(' ')[-1].strip())
            except:
                print(('Job submission failed: ' + output))
        except:
            print(('Job submission failed: ' + cmd_line))

    return job_id


def delete(job_id):
    with os.popen('scancel ' + job_id) as f:
        pass


def status():
    status_dict = {}
    with os.popen('squeue -u `whoami`') as f:
        try:
            f.readline()  # read header
            for line in f:
                new_line = re.sub(' +', ' ', line.strip())
                job_id = int(new_line.split(' ')[0])
                state = new_line.split(' ')[4]
                if state == 'R':
                    status_dict[job_id] = 'r'
                else:
                    status_dict[job_id] = state
        except Exception as e:
            print(e)

    return status_dict


def job_stats(job_id):
    stats_dict = {}
    with os.popen('sacct --noheader --format JobId,Elapsed,TotalCPU,Partition,NTasks,AveRSS,State,ExitCode -P -j ' + str(job_id)) as f:
        try:
            line = f.readline()
            first_line = line.split('|')

            line = f.readline()
            if len(line) > 0:
                next_line = line.split('|')

            wallclock_str = first_line[1]
            stats_dict['wallclock'] = get_timedelta(wallclock_str)

            cpu_str = first_line[2]
            stats_dict['cpu'] = get_timedelta(cpu_str)

            if len(first_line[3]) > 0:
                stats_dict['queue'] = first_line[3]
            elif next_line:
                stats_dict['queue'] = next_line[3]

            if len(first_line[4]) > 0:
                stats_dict['ntasks'] = int(first_line[4])
            elif next_line:
                stats_dict['ntasks'] = int(next_line[4])

            if len(first_line[6]) > 0:
                stats_dict['status'] = first_line[6]
            elif next_line:
                stats_dict['status'] = next_line[6]

            if len(first_line[7]) > 0:
                stats_dict['exit_code'] = int(first_line[7].split(':')[0])
            elif next_line:
                stats_dict['exit_code'] = int(next_line[7].split(':')[0])

            # stats_dict['mem'] = 0 #float(new_line.split('
            # ')[4])*int(new_line.split(' ')[3])
        except:
            print('SLURM: Error reading job stats')

    with os.popen('squeue --format %%S -h -j ' + str(job_id)) as f:
        try:
            line = f.readline()
            if len(line) > 0:
                stats_dict['start_time'] = line
            else:
                stats_dict['start_time'] = ""
        except:
            print('SLURM: Error getting start time')
    return stats_dict


def job_stats_enhanced(job_id):
    """
    Get full job and step stats for job_id
    """
    stats_dict = {}
    with os.popen('sacct --noheader --format JobId,Elapsed,TotalCPU,Partition,NTasks,AveRSS,State,ExitCode,start,end -P -j ' + str(job_id)) as f:
        try:
            line = f.readline()
            if line in ["SLURM accounting storage is disabled",
                        "slurm_load_job error: Invalid job id specified"]:
                raise
            cols = line.split('|')
            stats_dict['job_id'] = cols[0]
            stats_dict['wallclock'] = get_timedelta(cols[1])
            stats_dict['cpu'] = get_timedelta(cols[2])
            stats_dict['queue'] = cols[3]
            stats_dict['status'] = cols[6]
            stats_dict['exit_code'] = cols[7].split(':')[0]
            stats_dict['start'] = cols[8]
            stats_dict['end'] = cols[9]

            steps = []
            for line in f:
                step = {}
                cols = line.split('|')
                step_val = cols[0].split('.')[1]
                step['step'] = step_val
                step['wallclock'] = get_timedelta(cols[1])
                step['cpu'] = get_timedelta(cols[2])
                step['ntasks'] = cols[4]
                step['status'] = cols[6]
                step['exit_code'] = cols[7].split(':')[0]
                step['start'] = cols[8]
                step['end'] = cols[9]
                steps.append(step)
            stats_dict['steps'] = steps
        except:
            with os.popen('squeue -h -j %s' % str(job_id)) as f:
                try:
                    for line in f:
                        new_line = re.sub(' +', ' ', line.strip())
                        job_id = int(new_line.split(' ')[0])
                        state = new_line.split(' ')[4]
                        stats_dict['job_id'] = str(job_id)
                        stats_dict['status'] = state
                except:
                    print('SLURM: Error reading job stats')
                    stats_dict['status'] = 'UNKNOWN'
    with os.popen('squeue --format %%S -h -j ' + str(job_id)) as f:
        try:
            line = f.readline()
            if len(line) > 0:
                stats_dict['start_time'] = line
            else:
                stats_dict['start_time'] = ""
        except:
            print('SLURM: Error getting start time')
    return stats_dict


def is_in_queue(job_id):
    with os.popen('squeue -j %s' % job_id) as f:
        try:
            f.readline()  # read header
            for line in f:
                new_line = re.sub(' +', ' ', line.strip())
                q_id = int(new_line.split(' ')[0])
                if q_id == job_id:
                    return True
        except e:
            pass
    return False


def running_stats(job_id):
    stats_dict = {}
    with os.popen('sacct --noheader --format Elapsed -j ' + str(job_id)) as f:
        try:
            line = f.readline()
            new_line = re.sub(' +', ' ', line.strip())
            stats_dict['wallclock'] = get_timedelta(new_line)
        except:
            pass

    with os.popen('sstat --noheader --format AveCPU,AveRSS,NTasks -j ' + str(job_id)) as f:
        try:
            line = f.readline()
            new_line = re.sub(' +', ' ', line.strip())
            ntasks = int(new_line.split(' ')[2])
            stats_dict['mem'] = (
                float(new_line.split(' ')[1].replace('K', '')) * ntasks)
            stats_dict['cpu'] = '-'  # float(new_line.split(' ')[0])*ntasks
        except:
            pass

    return stats_dict
