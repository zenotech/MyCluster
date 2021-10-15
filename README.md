# MyCluster
[![PyPI version](https://badge.fury.io/py/mycluster.svg)](https://badge.fury.io/py/mycluster)

Library and command line interface to support interacting with multiple HPC clusters  

Provides the ability to interact with the most popular HPC job scheduling systems using a single interface 
and enables the creation of job submission scripts. 
 
Tested with SGE, LSF and slurm (PBS/TORQUE support under development)

## Getting started
PyEpic can be installed from PyPi.

```
pip install pyepic
```

## Configuration

#### Storing your email address
MyCluster will write your email address into any submission files so you can recieve updates from the schedulers. You can supply this on the command line or store it in a configuration file.
To store your email in the configuration file run:
```
mycluster configure
```

#### Setting a custom scheduler
By default MyCluster will try and detect the underlying scheduler but this can be overridden by setting the MYCLUSTER_SCHED environment variable. This should be set to a string name of a Python class that implements the `mycluster.schedulers.base.Scheduler` class.

#### Override the submission template
In some cases you may want to override the submission templates, for example if you want to include additional parameters or scheduler commands. To do this set the MYCLUSTER_TEMPLATE environment variable to the jinja template you wish to use. See mycluster/schedulers/templates for the base templates.

## Command Line
MyClusyter installs the "mycluster" cli command to interact with the local scheduler via the command line.

Print command help
```
mycluster <command> --help
```

List all queues
```
mycluster queues
```

List jobs 
```
mycluster list
```

Create a new submission file, see --help for more submission options.
```
mycluster create JOBFILE QUEUE RUNSCRIPT
```

Submit a job file
```
mycluster submit JOBFILE
```

Cancel a job
```
mycluster cancel JOBID
```

The RUNSCRIPT to be executed by the JOB_SCRIPT can make use of the following predefined environment variables
```bash
export NUM_TASKS=
export TASKS_PER_NODE=
export THREADS_PER_TASK=
export NUM_NODES=

# OpenMP configuration
export OMP_NUM_THREADS=$THREADS_PER_TASK

# Default mpiexec commnads for each flavour of mpi
export OMPI_CMD="mpiexec -n $NUM_TASKS -npernode $TASKS_PER_NODE -bysocket -bind-to-socket"
export MVAPICH_CMD="mpiexec -n $NUM_TASKS -ppn $TASKS_PER_NODE -bind-to-socket"
export IMPI_CMD="mpiexec -n $NUM_TASKS -ppn $TASKS_PER_NODE"
```


## API
Mycluster can be used programatically using the mycluster module. All schedulers implement the base `mycluster.schedulers.base.Scheduler` class.

```python
import mycluster

# Detect the local scheduler
scheduler = mycluster.detect_scheduling_sys()

print(f"Scheduler loaded: {scheduler.scheduler_type()}")

# Create a batch script to submit a 48 task run of script.sh to the skylake queue
script = scheduler.create("skylake", 48, "my_job", "script.sh", "01:00:00", tasks_per_node=24)

# Write to a file
with open("mysub.job", "w") as f:
    f.write(script)

# Submit the batch script
job_id = scheduler.submit("mysub.job")

# Check the status of the job
print(scheduler.get_job_details(job_id))

# Cancel the job
scheduler.delete(job_id)

```
