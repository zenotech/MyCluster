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
import sys
import logging
from abc import ABC, abstractmethod
from jinja2 import Environment, FileSystemLoader
from datetime import timedelta
from subprocess import check_output

logger = logging.getLogger(__name__)


class Scheduler(ABC):
    """
    Abstract base class for a batch scheduler interface
    """

    @abstractmethod
    def scheduler_type(self):
        """
        Returns the scheduler type
        """
        pass

    @abstractmethod
    def name(self):
        """
        Fetch the cluster name
        """
        pass

    @abstractmethod
    def queues(self):
        """
        List the available queues
        """
        pass

    @abstractmethod
    def node_config(self, queue_id):
        """
        Fetch the hardware config for nodes in queue with queue_id
        """
        pass

    @abstractmethod
    def tasks_per_node(self, queue_id):
        """
        How many tasks can be run per node in queue with queue_id
        """
        pass

    @abstractmethod
    def available_tasks(self, queue_id):
        """
        Get the current status of the cluster
        """
        pass

    @abstractmethod
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
        """
        Write a new job file
        """
        pass

    @abstractmethod
    def submit(
        self, script_name, immediate=False, depends_on=None, depends_on_always_run=False
    ):
        """
        Submit the job file script_name to the scheduler
        """
        pass

    @abstractmethod
    def list_current_jobs(self):
        """
        List all the jobs currently active with the scheduler
        """
        pass

    @abstractmethod
    def get_job_details(self, job_id):
        """
        Get the details of job with ID job_id
        """
        pass

    @abstractmethod
    def delete(self, job_id):
        """
        Delete job with ID job_id
        """
        pass

    def accounts(self):
        """
        List billings accounts configured in scheduler
        """
        return []

    def _get_template_name(self):
        if "MYCLUSTER_TEMPLATE" in os.environ:
            logger.debug(
                f'MYCLUSTER_TEMPLATE set, loading template from {os.environ["MYCLUSTER_TEMPLATE"]}'
            )
            return os.environ["MYCLUSTER_TEMPLATE"]
        else:
            return f"{self.scheduler_type()}.jinja"

    def _load_template(self, template_name):
        """
        Load the jinja2 template with name template_name
        1. Check if template_name is absolute
        2. Try loading it from templates dir
        """
        if os.path.isfile(template_name):
            logger.debug(f'File "{template_name}" exists, loading it.')
            env = Environment(loader=FileSystemLoader(os.path.dirname(template_name)))
            return env.get_template(os.path.basename(template_name))
        else:
            logger.debug(
                f'Loading template "{template_name}" from "{os.path.join(os.path.dirname(__file__), "templates")}"'
            )
            env = Environment(
                loader=FileSystemLoader(
                    os.path.join(os.path.dirname(__file__), "templates")
                )
            )
            return env.get_template(template_name)

    def _get_data(self, filename):
        """
        Helper function to find the application specific run-scripts
        """
        packagedir = os.path.dirname(__file__)
        # Need to traverse lib/python3.x/site-packages/mycluster
        # So 4 directories above the current packagedir
        dirname = os.path.join(packagedir, "..", "..", "..", "..", "share", "MyCluster")
        fullname = os.path.join(dirname, filename)

        if not os.path.isfile(fullname):
            dirname = os.path.join(packagedir, "..", "share", "MyCluster")
            fullname = os.path.join(dirname, filename)
        # Need to check if file exists as
        # share location may also be sys.prefix/share
        if not os.path.isfile(fullname):
            dirname = os.path.join(sys.prefix, "share", "MyCluster")
            fullname = os.path.join(dirname, filename)

        if not os.path.isfile(fullname):
            raise Exception(f"Unable to find file {fullname}")

        logger.debug(f'Loading data file from "{fullname}"')
        return fullname

    def _get_timedelta(self, date_str):
        """
        Converts date_str from string in [DD-[hh:]]mm:ss format to a timedelta object
        """
        days = 0
        hours = 0
        minutes = 0
        seconds = 0

        if date_str.count("-") == 1:
            days = int(date_str.split("-")[0])
            date_str = date_str.partition("-")[2]
        if date_str.count(":") == 2:
            hours = int(date_str.split(":")[0])
            date_str = date_str.partition(":")[2]

        try:
            minutes = int(date_str.split(":")[0])
            seconds = int(date_str.split(":")[1])
        except:
            pass

        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    def _check_output(*args, **kwargs):
        """
        check_output wrapper that decodes to a str instead of bytes
        """
        return check_output(*args, **kwargs).decode("UTF-8")
