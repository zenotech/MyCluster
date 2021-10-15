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


class LSF(Scheduler):
    def scheduler_type(self):
        return "lsf"

    def name(self):
        lsid_output = self._check_output(["lsid"]).splitlines()
        for line in lsid_output:
            if line.startswith("My cluster name is"):
                return line.rsplit(" ", 1)[1].strip()
        return "undefined"

    def queues(self):
        queue_list = []
        with os.popen("bqueues -w -u `whoami`") as f:
            f.readline()  # read header
            for line in f:
                q = line.split(" ")[0].strip()
                queue_list.append(q)
        return queue_list

    def node_config(self, queue_id):
        # Find first node with queue and record node config
        # bqueues -l queue_id
        host_list = None
        config = {}
        q_output = self._check_output(["bqueues", "-l", queue_id]).splitlines()
        for line in q_output:
            if line.startswith("HOSTS:"):
                host_list = line.strip().rsplit(" ", 1)[1].replace("/", "")
                if host_list == "none":
                    config["max task"] = 0
                    config["max thread"] = 0
                    config["max memory"] = 0
                    return config
        bhosts_output = self._check_output(["bhosts", "-l", host_list]).splitlines()
        line = re.sub(" +", " ", bhosts_output[2]).strip()
        tasks = int(line.split(" ")[3])
        line = re.sub(" +", " ", bhosts_output[6]).strip()
        memory = float(line.split(" ")[11].replace("G", ""))
        config["max task"] = tasks
        config["max thread"] = tasks
        config["max memory"] = memory
        return config

    def tasks_per_node(self, queue_id):
        host_list = None
        q_output = self._check_output()(["bqueues", "-l", queue_id]).splitlines()
        for line in q_output:
            if line.startswith("HOSTS:"):
                host_list = line.strip().rsplit(" ", 1)[1].replace("/", "")
                if host_list == "none":
                    return 0
        bhosts_output = self._check_output(["bhosts", "-l", host_list]).splitlines()
        line = re.sub(" +", " ", bhosts_output[2]).strip()
        tasks = int(line.split(" ")[3])
        return tasks

    def available_tasks(self, queue_id):
        # split queue id into queue and parallel env
        # list free slots
        free_tasks = 0
        max_tasks = 0
        run_tasks = 0
        queue_name = queue_id
        q_output = self._check_output(["bqueues", queue_name]).splitlines()
        for line in q_output:
            if line.startswith(queue_name):
                new_line = re.sub(" +", " ", line).strip()
                try:
                    max_tasks = int(new_line.split(" ")[4])
                except:
                    pass
                pen_tasks = int(new_line.split(" ")[8])
                run_tasks = int(new_line.split(" ")[9])
                sus_tasks = int(new_line.split(" ")[10])
        return {"available": max_tasks - run_tasks, "max tasks": max_tasks}

    def create_submit(
        self,
        queue_id,
        num_tasks,
        job_name,
        job_script,
        wall_clock,
        openmpi_arg="-bysocket -bind-to-socket",
        project_name="default",
        tasks_per_node=None,
        threads_per_task=1,
        user_email=None,
        qos=None,
        exclusive=True,
        output_name=None,
    ):
        if tasks_per_node is None:
            tasks_per_node = self.tasks_per_node(queue_id)
        num_nodes = int(math.ceil(float(num_tasks) / float(tasks_per_node)))

        if threads_per_task is None:
            threads_per_task = 1

        if ":" not in wall_clock:
            wall_clock = wall_clock + ":00:00"

        if "mycluster-" in job_script:
            my_script = self._get_data(my_script)

        num_queue_slots = num_nodes * tasks_per_node(queue_id)

        if output_name is None:
            output_name = job_name + ".out"

        template = self._load_template("lsf.jinja")

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
            exclusive=exclusive,
        )

        return script_str

    def submit(
        self, script_name, immediate=False, depends_on=None, depends_on_always_run=False
    ):
        job_id = None
        if immediate:
            raise NotYetImplementedException("Immediate not yet implemented for LSF")
        if depends_on and depends_on_always_run:
            cmd = 'bsub -w "ended(%s)" < %s ' % (depends_on, script_name)
            with os.popen(cmd) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(" ")[1].replace("<", "").replace(">", ""))
                except:
                    raise SchedulerException("Job submission failed: " + output)
        elif depends_on is not None:
            cmd = 'bsub -w "done(%s)" < %s ' % (depends_on, script_name)
            with os.popen(cmd) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(" ")[1].replace("<", "").replace(">", ""))
                except:
                    raise SchedulerException("Job submission failed: " + output)
        else:
            with os.popen("bsub <" + script_name) as f:
                output = f.readline()
                try:
                    job_id = int(output.split(" ")[1].replace("<", "").replace(">", ""))
                except:
                    raise SchedulerException("Job submission failed: " + output)
        return job_id

    def list_current_jobs(self):
        jobs = []
        output = subprocess.run(
            'bjobs -u `whoami` -o "jobid queue job_name stat"',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        if output.returncode == 0:
            for line in output.stdout.decode("utf-8").splitlines():
                if line == "No unfinished job found":
                    return jobs
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
        """
        stats_dict = {}
        with os.popen(
            "bjobs -o \"jobid run_time cpu_used  queue slots  stat exit_code start_time estimated_start_time finish_time delimiter='|'\" -noheader "
            + str(job_id)
        ) as f:
            try:
                line = f.readline()
                cols = line.split("|")
                stats_dict["job_id"] = cols[0]
                if cols[1] != "-":
                    stats_dict["wallclock"] = timedelta(
                        seconds=float(cols[1].split(" ")[0])
                    )
                if cols[2] != "-":
                    stats_dict["cpu"] = timedelta(seconds=float(cols[2].split(" ")[0]))
                stats_dict["queue"] = cols[3]
                stats_dict["status"] = cols[5]
                stats_dict["exit_code"] = cols[6]
                stats_dict["start"] = cols[7]
                stats_dict["start_time"] = cols[8]
                if stats_dict["status"] in ["DONE", "EXIT"]:
                    stats_dict["end"] = cols[9]

                steps = []
                stats_dict["steps"] = steps
            except:
                with os.popen("bhist -l " + str(job_id)) as f:
                    try:
                        output = f.readlines()
                        for line in output:
                            if "Done successfully" in line:
                                stats_dict["status"] = "DONE"
                                return stats_dict
                            elif "Completed <exit>" in line:
                                stats_dict["status"] = "EXIT"
                                return stats_dict
                            else:
                                stats_dict["status"] = "UNKNOWN"
                    except Exception as e:
                        raise SchedulerException(
                            "Error fetching job details from bjobs, check job id."
                        )
        return stats_dict

    def delete(self, job_id):
        cmd = f"bkill {job_id}"
        output = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        if output.returncode != 0:
            raise SchedulerException(f"Error cancelling job {job_id}")
