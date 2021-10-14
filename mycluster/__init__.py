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
import importlib
import logging

from subprocess import check_output, CalledProcessError

from .exceptions import ConfigurationException

logger = logging.getLogger(__name__)

_available_schedulers = {
    "slurm": "mycluster.schedulers.slurm.Slurm",
    "pbs": "mycluster.schedulers.pbs.PBS",
    "lsf": "mycluster.schedulers.lsf.LSF",
    "sge": "mycluster.schedulers.sge.SGE",
}


def my_import(name):
    logger.debug(f"Importing schduler class {name}")
    module = mod = name.rsplit(".", 1)[0]
    klass = mod = name.rsplit(".", 1)[1]
    mod = importlib.import_module(module)
    return getattr(mod, klass)


def get_scheduler(scheduler_name):
    logger.debug(f"Getting implementation of {scheduler_name}")
    if scheduler_name in _available_schedulers:
        return my_import(_available_schedulers[scheduler_name])()
    else:
        raise ConfigurationException(f"Invalid scheduler specified: {scheduler_name}")


def detect_scheduler_name():
    """
    Try to automatically detect scheduler type
    """
    # Test for custom scheduler
    if os.getenv("MYCLUSTER_SCHED") is not None:
        logger.debug(f"Custom scheduler defined using MYCLUSTER_SCHED")
        return os.getenv("MYCLUSTER_SCHED")

    # Test for SLURM
    if os.getenv("SLURMHOME") is not None:
        logger.debug(f"SLURMHOME set, Slurm assumed")
        return "slurm"

    try:
        line = check_output(["scontrol", "ping"], universal_newlines=True)
        if line.split("(")[0] == "Slurmctld":
            logger.debug(f"scontrol responded, Slurm assumed")
            return "slurm"
    except (CalledProcessError, FileNotFoundError) as e:
        pass

    # Test for PBS
    try:
        line = check_output(["pbsnodes", "-a"])
        logger.debug(f"pbsnodes responded, PBS assumed")
        return "pbs"
    except (CalledProcessError, FileNotFoundError) as e:
        pass

    # Test for SGE
    if os.getenv("SGE_CLUSTER_NAME") is not None:
        logger.debug(f"SGE_CLUSTER_NAME set, SGE assumed")
        return "sge"

    # Test for lsf
    try:
        line = check_output("lsid")
        if line.split(" ")[0] == "Platform" or line.split(" ")[0] == "IBM":
            logger.debug(f"lsid responded, LSF assumed")
            return "lsf"
    except (CalledProcessError, FileNotFoundError) as e:
        pass

    logger.debug(f"All detection tests failed, unabled to detect local scheduler")
    raise ConfigurationException("Error, unabled to detect local scheduler")


def detect_scheduling_sys():
    """
    Returns the scheduler implementation
    """
    name = detect_scheduler_name()
    return get_scheduler(name)
