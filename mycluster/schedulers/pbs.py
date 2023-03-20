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
from mycluster.exceptions import (
    SchedulerException,
    ConfigurationException,
    NotYetImplementedException,
)


class PBS(Scheduler):
    def scheduler_type(self):
        return "pbs"

    def name(self):
        raise NotYetImplementedException("name() not implemented for PBS")

    def queues(self):
        queue_list = []
        try:
            output = self._check_output(["qstat", "-Q"])
            lines = output.splitlines()[2:]
            for queue in lines:
                queue_list.append(queue.split(" ")[0])
        except Exception as e:
            raise SchedulerException("Error fetching queues")
        return queue_list

    def _get_vnode_name(self, queue_id):
        try:
            output = self._check_output(["qstat", "-Qf", queue_id])
            for line in output.splitlines():
                if line.strip().startswith("default_chunk.vntype"):
                    return line.split("=")[-1].strip()
        except Exception as e:
            raise SchedulerException("Error fetching node config")

    def node_config(self, queue_id):
        max_threads = 1
        max_memory = 1
        try:
            tpn = tasks_per_node(queue_id)
            vnode_type = vnode_type = vnode_type = self._get_vnode_name(queue_id)
            if vnode_type is not None:
                output = self._check_output(
                    "pbsnodes -a -F dsv | grep {}".format(vnode_type), shell=True
                )
            for line in output.splitlines():
                for item in line.split("|"):
                    [key, value] = item.strip().split("=")
                    if key.strip() == "resources_available.vps_per_ppu":
                        if int(value) > max_threads:
                            max_threads = int(value) * tpn
                    if key.strip() == "resources_available.mem":
                        # strip kb and convert to mb
                        mem = float(value[:-2]) / 1024
                        if mem > max_memory:
                            max_memory = mem
        except Exception as e:
            raise SchedulerException("Error fetching node config")
        return {"max thread": max_threads, "max memory": max_memory}

    def tasks_per_node(self, queue_id):
        tpn = 1
        try:
            vnode_type = vnode_type = self._get_vnode_name(queue_id)
            if vnode_type is not None:
                output = self._check_output(
                    "pbsnodes -a -F dsv | grep {}".format(vnode_type), shell=True
                )
                for line in output.splitlines():
                    for item in line.split("|"):
                        [key, value] = item.strip().split("=")
                        if key.strip() == "resources_available.ncpus":
                            if int(value) > tpn:
                                tpn = int(value)
        except Exception as e:
            raise SchedulerException("Error fetching node config")
        return tpn

    def available_tasks(self, queue_id):
        free_tasks = 0
        max_tasks = 0
        assigned_tasks = 0
        try:
            vnode_type = self._get_vnode_name(queue_id)
            if vnode_type is not None:
                output = self._check_output(
                    "pbsnodes -a -F dsv | grep {}".format(vnode_type), shell=True
                )
            for line in output.splitlines():
                for item in line.split("|"):
                    [key, value] = item.strip().split("=")
                    if key.strip() == "resources_available.ncpus":
                        max_tasks += int(value)
                    elif key.strip() == "resources_assigned.ncpus":
                        assigned_tasks += int(value)
            free_tasks = max_tasks - assigned_tasks
        except Exception as e:
            raise SchedulerException("Error fetching node config")
        return {"available": free_tasks, "max tasks": max_tasks}

    def _min_tasks_per_node(self, queue_id):
        return 1

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
            threads_per_task = 1

        if "mycluster-" in my_script:
            my_script = self._get_data(my_script)

        if ":" not in wall_clock:
            wall_clock = wall_clock + ":00:00"

        num_nodes = int(math.ceil(float(num_tasks) / float(tasks_per_node)))

        num_queue_slots = num_nodes * queue_tpn

        if not exclusive:
            if num_nodes == 1:  # Assumes fill up rule
                num_queue_slots = num_nodes * max(
                    tasks_per_node, self._min_tasks_per_node(queue_id)
                )

        if output_name is None:
            output_name = job_name + ".out"

        template = self._load_template("pbs.jinja")

        script_str = template.render(
            my_name=job_name,
            my_script=job_script,
            my_output=output_name,
            user_email=user_email,
            queue_name=queue_name,
            num_queue_slots=num_queue_slots,
            num_tasks=num_tasks,
            tpn=tasks_per_node,
            num_threads_per_task=threads_per_task,
            num_nodes=num_nodes,
            project_name=project_name,
            wall_clock=wall_clock,
            openmpi_args=openmpi_args,
            qos=qos,
        )

        return script_str

    def submit(
        self, script_name, immediate=False, depends_on=None, depends_on_always_run=False
    ):
        job_id = None
        if not immediate:
            if depends_on and depends_on_always_run:
                with os.popen(
                    "qsub -W depend=afterany:%s %s" % (depends_on, script_name)
                ) as f:
                    output = f.readline()
                    try:
                        job_id = output.strip().split(".")[0]
                    except:
                        raise SchedulerException("Job submission failed: " + output)
            elif depends_on is not None:
                with os.popen(
                    "qsub -W depend=afterok:%s %s" % (depends_on, script_name)
                ) as f:
                    output = f.readline()
                    try:
                        job_id = output.strip().split(".")[0]
                    except:
                        raise SchedulerException("Job submission failed: " + output)
            else:
                with os.popen("qsub " + script_name) as f:
                    output = f.readline()
                    try:
                        job_id = output.strip().split(".")[0]
                    except:
                        raise SchedulerException("Job submission failed: " + output)
        else:
            raise NotYetImplementedException("Immediate not yet implemented for PBS")
        return job_id

    def list_current_jobs(self):
        raise NotYetImplementedException(
            "list_current_jobs not yet implemented for PBS"
        )

    def get_job_details(self, job_id):
        """
        Get full job and step stats for job_id
        """
        stats_dict = {}
        with os.popen("qstat -xf " + str(job_id)) as f:
            try:
                line = f.readline().strip()
                while line:
                    if line.startswith("Job Id:"):
                        stats_dict["job_id"] = line.split(":")[1].split(".")[0].strip()
                    elif line.startswith("resources_used.walltime"):
                        stats_dict["wallclock"] = get_timedelta(line.split("=")[1])
                    elif line.startswith("resources_used.cput"):
                        stats_dict["cpu"] = get_timedelta(line.split("=")[1])
                    elif line.startswith("queue"):
                        stats_dict["queue"] = line.split("=")[1].strip()
                    elif line.startswith("job_state"):
                        stats_dict["status"] = line.split("=")[1].strip()
                    elif line.startswith("Exit_status"):
                        stats_dict["exit_code"] = line.split("=")[1].strip()
                    elif line.startswith("Exit_status"):
                        stats_dict["exit_code"] = line.split("=")[1].strip()
                    elif line.startswith("stime"):
                        stats_dict["start"] = line.split("=")[1].strip()
                    line = f.readline().strip()
                if stats_dict["status"] == "F" and "exit_code" not in stats_dict:
                    stats_dict["status"] = "CA"
                elif stats_dict["status"] == "F" and stats_dict["exit_code"] == "0":
                    stats_dict["status"] = "PBS_F"
            except Exception as e:
                with os.popen("qstat -xaw " + str(job_id)) as f:
                    try:
                        output = f.readlines()
                        for line in output:
                            if str(job_id) in line:
                                cols = line.split()
                                stats_dict["job_id"] = cols[0].split(".")[0]
                                stats_dict["queue"] = cols[2]
                                stats_dict["status"] = cols[9]
                                stats_dict["wallclock"] = get_timedelta(cols[10])
                                return stats_dict
                    except Exception as e:
                        stats_dict["status"] = "UNKNOWN"
        return stats_dict

    def delete(self, job_id):
        cmd = f"qdel {job_id}"
        output = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        if output.returncode != 0:
            raise SchedulerException(f"Error cancelling job {job_id}")
