# BSD 3 - Clause License

# Copyright(c) 2021, Zenotech
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.

# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and / or other materials provided with the distribution.

# 3. Neither the name of the copyright holder nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#         SERVICES
#         LOSS OF USE, DATA, OR PROFITS
#         OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import os
import re
import math
import subprocess

from .base import Scheduler
from mycluster.exceptions import SchedulerException, ConfigurationException


class Slurm(Scheduler):
    def scheduler_type(self):
        return "slurm"

    def name(self):
        with os.popen("sacctmgr show cluster") as f:
            f.readline()
            f.readline()
            return f.readline().strip().split(" ")[0]

    def queues(self):
        queue_list = []
        with os.popen("sinfo -sh") as f:
            for line in f:
                q = line.split(" ")[0].strip().replace("*", "")
                queue_list.append(q)
        return queue_list

    def node_config(self, queue_id):
        # Find first node with queue and record node config
        queue_name = queue_id
        tasks = 0
        config = {}
        with os.popen("sinfo -Nelh -p " + queue_name) as f:
            line = f.readline()
            if len(line):
                new_line = re.sub(" +", " ", line.strip())
                tasks = int(new_line.split(" ")[4])
                memory = int(new_line.split(" ")[6])
                config["max task"] = tasks
                config["max thread"] = tasks
                config["max memory"] = memory
            else:
                raise SchedulerException(
                    "Requested partition %s has no nodes" % queue_name
                )
        return config

    def tasks_per_node(self, queue_id):
        queue_name = queue_id
        tasks = 0
        with os.popen("sinfo -Nelh -p " + queue_name) as f:
            line = f.readline()
            new_line = re.sub(" +", " ", line.strip())
            tasks = int(new_line.split(" ")[4])
        return tasks

    def available_tasks(self, queue_id):
        free_tasks = 0
        max_tasks = 0
        queue_name = queue_id
        nc = self.node_config(queue_id)
        with os.popen("sinfo -sh -p " + queue_name) as f:
            line = f.readline()
            new_line = re.sub(" +", " ", line.strip())
            line = new_line.split(" ")[3]
            free_tasks = int(line.split("/")[1]) * nc["max task"]
            max_tasks = int(line.split("/")[3]) * nc["max task"]
        return {"available": free_tasks, "max tasks": max_tasks}

    def accounts(self):
        account_list = []
        with os.popen(
            "sacctmgr --noheader list assoc user=`id -un` format=Account"
        ) as f:
            for line in f:
                account_list.append(line)
        return account_list

    def create_submit(
        self,
        queue_id,
        num_tasks,
        job_name,
        job_script,
        wall_clock,
        openmpi_args="-bysocket -bind-to-socket",
        project_name="default",
        tasks_per_node=None,
        threads_per_task=1,
        user_email=None,
        qos=None,
        exclusive=True,
        output_name=None,
    ):
        queue_name = queue_id
        if tasks_per_node is None:
            tasks_per_node = self.tasks_per_node(queue_id)
        num_nodes = int(math.ceil(float(num_tasks) / float(tasks_per_node)))
        if threads_per_task is None:
            threads_per_task = -1

        if ":" not in wall_clock:
            wall_clock = wall_clock + ":00:00"

        if "mycluster-" in job_script:
            job_script = self._get_data(job_script)

        if output_name is None:
            output_name = job_name + ".out"

        template = self._load_template(self._get_template_name())

        script_str = template.render(
            my_name=job_name,
            my_script=job_script,
            my_output=output_name,
            user_email=user_email,
            queue_name=queue_name,
            num_tasks=num_tasks,
            tpn=tasks_per_node,
            num_threads_per_task=threads_per_task,
            num_nodes=num_nodes,
            project_name=project_name,
            wall_clock=wall_clock,
            openmpi_args=openmpi_args,
            qos=qos,
            exclusive=exclusive,
        )
        return script_str

    def submit(
        self, script_name, immediate=False, depends_on=None, depends_on_always_run=False
    ):
        additional_cmd = ""
        if "MYCLUSTER_SUBMIT_OPT" in os.environ:
            additional_cmd = os.environ["MYCLUSTER_SUBMIT_OPT"]
        if not immediate:
            if depends_on and depends_on_always_run:
                command = f"sbatch {additional_cmd} --kill-on-invalid-dep=yes --dependency=afterany:{depends_on} {script_name}"
            elif depends_on is not None:
                command = f"sbatch {additional_cmd} --kill-on-invalid-dep=yes --dependency=afterok:{depends_on} {script_name}"
            else:
                command = f"sbatch {additional_cmd} {script_name}"
            print(f"running {command}")
            with os.popen(command) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(" ")[-1].strip())
                except:
                    raise SchedulerException("Job submission failed: " + output)
        else:
            with os.popen(
                'grep -- "SBATCH -p" ' + script_name + " | sed 's/#SBATCH//'"
            ) as f:
                partition = f.readline().rstrip()
            with os.popen(
                'grep -- "SBATCH --nodes" ' + script_name + " | sed 's/#SBATCH//'"
            ) as f:
                nnodes = f.readline().rstrip()
            with os.popen(
                'grep -- "SBATCH --ntasks" ' + script_name + " | sed 's/#SBATCH//'"
            ) as f:
                ntasks = f.readline().rstrip()
            with os.popen(
                'grep -- "SBATCH -A" ' + script_name + " | sed 's/#SBATCH//'"
            ) as f:
                project = f.readline().rstrip()
            with os.popen(
                'grep -- "SBATCH -J" ' + script_name + " | sed 's/#SBATCH//'"
            ) as f:
                job = f.readline().rstrip()

            cmd_line = (
                "salloc --exclusive "
                + nnodes
                + " "
                + partition
                + " "
                + ntasks
                + " "
                + project
                + " "
                + job
                + " bash ./"
                + script_name
            )
            try:
                output = subprocess.check_output(cmd_line, shell=True)
                try:
                    job_id = int(output.split(" ")[-1].strip())
                except:
                    raise SchedulerException("Job submission failed: " + output)
            except:
                raise SchedulerException("Job submission failed: " + cmd_line)
        return job_id

    def list_current_jobs(self):
        jobs = []
        output = subprocess.run(
            "squeue -h -u `whoami`",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        if output.returncode == 0:
            for line in output.stdout.decode("utf-8").splitlines():
                job_info = re.sub(" +", " ", line.strip()).split(" ")
                jobs.append(
                    {
                        "id": int(job_info[0]),
                        "queue": job_info[1],
                        "name": job_info[2],
                        "state": job_info[4],
                    }
                )
        else:
            raise SchedulerException("Error fetching job queue listing")
        return jobs

    def get_job_details(self, job_id):
        """
        Get full job and step stats for job_id
        First check using sacct, then fallback to squeue
        """
        stats_dict = {}
        sacct_cmd = f"sacct --noheader --format JobId,Elapsed,TotalCPU,Partition,NTasks,AveRSS,State,ExitCode,start,end -P -j {job_id}"
        squeue_cmd = f'squeue --format "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R %S" -h -j {job_id}'
        output = subprocess.run(
            sacct_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        if output.returncode != 0:
            raise SchedulerException("Error fetching job details from sacct")
        lines = output.stdout.decode("utf-8").splitlines()
        if len(lines) != 0:
            if lines[0] not in [
                "SLURM accounting storage is disabled",
                "slurm_load_jobs error: Invalid job id specified",
            ]:
                cols = lines[0].split("|")
                stats_dict["job_id"] = cols[0]
                stats_dict["wallclock"] = self._get_timedelta(cols[1])
                stats_dict["cpu"] = self._get_timedelta(cols[2])
                stats_dict["queue"] = cols[3]
                stats_dict["status"] = cols[6]
                stats_dict["exit_code"] = cols[7].split(":")[0]
                stats_dict["start"] = cols[8]
                stats_dict["end"] = cols[9]
                steps = []
                for line in lines[1:]:
                    step = {}
                    cols = line.split("|")
                    step_val = cols[0].split(".")[1]
                    step["step"] = step_val
                    step["wallclock"] = self._get_timedelta(cols[1])
                    step["cpu"] = self._get_timedelta(cols[2])
                    step["ntasks"] = cols[4]
                    step["status"] = cols[6]
                    step["exit_code"] = cols[7].split(":")[0]
                    step["start"] = cols[8]
                    step["end"] = cols[9]
                    steps.append(step)
                stats_dict["steps"] = steps
        else:
            output = subprocess.run(
                squeue_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
            )
            if output.returncode != 0:
                raise SchedulerException(
                    "Error fetching job details from squeue, check job id."
                )
            lines = output.stdout.decode("utf-8").splitlines()
            for line in lines:
                if line == "slurm_load_jobs error: Invalid job id specified":
                    raise SchedulerException("Invalid job id specified")
                new_line = re.sub(" +", " ", line.strip())
                job_id = int(new_line.split(" ")[0])
                state = new_line.split(" ")[4]
                stats_dict["job_id"] = str(job_id)
                stats_dict["status"] = state
                stats_dict["start_time"] = new_line.split(" ")[8]
        return stats_dict

    def delete(self, job_id):
        cmd = f"scancel {job_id}"
        output = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        if output.returncode != 0:
            raise SchedulerException(f"Error cancelling job {job_id}")
