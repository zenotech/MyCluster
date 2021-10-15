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
import datetime

from .base import Scheduler
from mycluster.exceptions import SchedulerException, ConfigurationException


class SGE(Scheduler):
    def scheduler_type(self):
        return "sge"

    def name(self):
        return os.getenv("SGE_CLUSTER_NAME")

    def queues(self):
        # list all parallel env
        # for parallel_env list queues associated
        # Find first node with queue and record node config
        queue_list = []
        parallel_env_list = []
        with os.popen("qconf -spl") as f:
            for line in f:
                parallel_env_list.append(line.strip())
        for parallel_env in parallel_env_list:
            with os.popen("qstat -pe " + parallel_env + " -U `whoami` -g c") as f:
                f.readline()  # read header
                f.readline()  # read separator
                for line in f:
                    queue_name = line.split(" ")[0].strip()
                    # Check if user has permission to use queue
                    with os.popen("qstat -g c -U `whoami` -q " + queue_name) as f2:
                        try:
                            f2.readline()
                            f2.readline()
                            if len(f2.readline()):
                                queue_list.append(parallel_env + ":" + queue_name)
                        except:
                            pass
        return queue_list

    def node_config(self, queue_id):
        # Find first node with queue and record node config
        parallel_env = queue_id.split(":")[0]
        queue_name = queue_id.split(":")[1]
        host_group = 0
        with os.popen("qconf -sq " + queue_name) as f:
            for line in f:
                if line.split(" ")[0] == "hostlist":
                    new_line = re.sub(" +", " ", line)
                    host_group = new_line.split(" ")[1]

        config = {}
        host_name = ""
        found = False
        if host_group[0] is "@":
            # Is a host group
            with os.popen("qconf -shgrp_resolved " + host_group) as f:
                for line in f:
                    for host_name in line.split(" "):
                        with os.popen("qhost -q -h " + host_name) as f:
                            header = f.readline()  # read header
                            f.readline()  # read separator
                            new_header = re.sub(" +", " ", header).strip()
                            # sge <=6.2u4 style
                            if (new_header.split(" ")[3]) == "LOAD":
                                for line in f:
                                    if line[0] != " ":
                                        name = line.split(" ")[0]
                                        if name != "global":
                                            new_line = re.sub(" +", " ", line).strip()
                                            if new_line.split(" ")[3] != "-":
                                                config["max task"] = int(
                                                    new_line.split(" ")[2]
                                                )
                                                config["max thread"] = int(
                                                    new_line.split(" ")[2]
                                                )
                                                config["max memory"] = new_line.split(
                                                    " "
                                                )[4]
                                                found = True
                                                break
                            else:
                                for line in f:
                                    if line[0] != " ":
                                        name = line.split(" ")[0]
                                        if name != "global":
                                            new_line = re.sub(" +", " ", line).strip()
                                            if new_line.split(" ")[3] != "-":
                                                config["max task"] = int(
                                                    new_line.split(" ")[4]
                                                )
                                                config["max thread"] = int(
                                                    new_line.split(" ")[5]
                                                )
                                                config["max memory"] = new_line.split(
                                                    " "
                                                )[7]
                                                found = True
                                                break
                        if found:
                            break
        else:
            # Is a host
            host_name = host_group
            with os.popen("qhost -q -h " + host_name) as f:
                header = f.readline()  # read header
                f.readline()  # read separator
                new_header = re.sub(" +", " ", header).strip()
                if (new_header.split(" ")[3]) == "LOAD":  # sge <=6.2u4 style
                    for line in f:
                        if line[0] != " ":
                            name = line.split(" ")[0]
                            if name != "global":
                                new_line = re.sub(" +", " ", line).strip()
                                if new_line.split(" ")[3] != "-":
                                    config["max task"] = int(new_line.split(" ")[2])
                                    config["max thread"] = int(new_line.split(" ")[2])
                                    config["max memory"] = new_line.split(" ")[4]
                                else:
                                    config["max task"] = 0
                                    config["max thread"] = 0
                                    config["max memory"] = 0
                else:
                    for line in f:
                        if line[0] != " ":
                            name = line.split(" ")[0]
                            if name != "global":
                                new_line = re.sub(" +", " ", line).strip()
                                if new_line.split(" ")[3] != "-":
                                    config["max task"] = int(new_line.split(" ")[4])
                                    config["max thread"] = int(new_line.split(" ")[5])
                                    config["max memory"] = new_line.split(" ")[7]
                                else:
                                    config["max task"] = 0
                                    config["max thread"] = 0
                                    config["max memory"] = 0
        return config

    def tasks_per_node(self, queue_id):
        parallel_env = queue_id.split(":")[0]
        queue_name = queue_id.split(":")[1]
        tasks = 0
        with os.popen("qconf -sq " + queue_name) as f:
            for line in f:
                if line.split(" ")[0] == "slots":
                    tasks = int(re.split("\W+", line)[1])

        pe_tasks = tasks
        with os.popen("qconf -sp " + parallel_env) as f:
            try:
                for line in f:
                    if line.split(" ")[0] == "allocation_rule":
                        try:
                            # This may throw exception as allocation rule
                            # may not always be an integer
                            pe_tasks = int(re.split("\W+", line)[1])
                        except ValueError as e:
                            raise SchedulerException("Error parsing SGE output")
            except:
                pass

        return min(tasks, pe_tasks)

    def available_tasks(self, queue_id):
        # split queue id into queue and parallel env
        # list free slots
        free_tasks = 0
        max_tasks = 0
        parallel_env = queue_id.split(":")[0]
        queue_name = queue_id.split(":")[1]
        with os.popen(" qstat -pe " + parallel_env + " -U `whoami` -g c") as f:
            f.readline()  # read header
            f.readline()  # read separator
            for line in f:
                # remove multiple white space
                new_line = re.sub(" +", " ", line)
                qn = new_line.split(" ")[0]
                if qn == queue_name:
                    free_tasks = int(new_line.split(" ")[4])
                    max_tasks = int(new_line.split(" ")[5])

        return {"available": free_tasks, "max tasks": max_tasks}

    def _min_tasks_per_node(self, queue_id):
        """
        This function is used when requesting non exclusive use
        as the parallel environment might enforce a minimum number
        of tasks
        """
        parallel_env = queue_id.split(":")[0]
        queue_name = queue_id.split(":")[1]
        tasks = 1
        pe_tasks = tasks
        with os.popen("qconf -sp " + parallel_env) as f:
            try:
                for line in f:
                    if line.split(" ")[0] == "allocation_rule":
                        # This may throw exception as allocation rule
                        # may not always be an integer
                        pe_tasks = int(re.split("\W+", line)[1])
            except:
                pass

        return max(tasks, pe_tasks)

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
        parallel_env = queue_id.split(":")[0]
        queue_name = queue_id.split(":")[1]

        if tasks_per_node is None:
            tasks_per_node = self.tasks_per_node(queue_id)

        num_nodes = int(math.ceil(float(num_tasks) / float(tasks_per_node)))
        if threads_per_task is None:
            threads_per_task = 1

        if ":" not in wall_clock:
            wall_clock = wall_clock + ":00:00"

        if "mycluster-" in job_script:
            job_script = self._get_data(job_script)

        if output_name is None:
            output_name = job_name + ".out"

        # For exclusive node use total number of slots required
        # is number of nodes x number of slots offer by queue
        num_queue_slots = num_nodes * self.tasks_per_node(queue_id)

        if not exclusive:
            if num_nodes == 1:  # Assumes fill up rule
                num_queue_slots = max(
                    tasks_per_node, self._min_tasks_per_node(queue_id)
                )

        template = load_template("sge.jinja")

        script_str = template.render(
            my_name=job_name,
            my_script=job_script,
            my_output=output_name,
            user_email=user_email,
            queue_name=queue_name,
            parallel_env=parallel_env,
            num_queue_slots=num_queue_slots,
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
        job_id = None
        with os.popen("qsub -V -terse " + script_name) as f:
            job_id = 0
            try:
                job_id = int(f.readline().strip())
            except:
                raise SchedulerException("Error submitting job to SGE")
        return job_id

    def list_current_jobs(self):
        jobs = []
        output = subprocess.run(
            "qstat -u `whoami`",
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
                        "queue": job_info[6],
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
        output = {}
        with os.popen("qacct -j " + str(job_id)) as f:
            try:
                f.readline()  # read header
                for line in f:
                    new_line = re.sub(" +", " ", line.strip())
                    output[new_line.split(" ")[0]] = new_line.split(" ", 1)[1]
            except:
                pass
        stats_dict["wallclock"] = datetime.timedelta(
            seconds=int(output["ru_wallclock"])
        )
        stats_dict["mem"] = output["mem"]
        stats_dict["cpu"] = datetime.timedelta(seconds=int(output["cpu"].split(".")[0]))
        stats_dict["queue"] = output["granted_pe"] + ":" + output["qname"]
        return stats_dict

    def delete(self, job_id):
        cmd = f"qdel {job_id}"
        output = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        if output.returncode != 0:
            raise SchedulerException(f"Error cancelling job {job_id}")
