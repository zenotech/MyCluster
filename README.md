MyCluster
=========
Master [![Build Status](https://travis-ci.org/zenotech/MyCluster.svg?branch=master)](https://travis-ci.org/zenotech/MyCluster) [![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/zenotech/MyCluster/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/zenotech/MyCluster/?branch=master)

Develop [![Build Status](https://travis-ci.org/zenotech/MyCluster.svg?branch=develop)](https://travis-ci.org/zenotech/MyCluster) [![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/zenotech/MyCluster/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/zenotech/MyCluster/?branch=develop)

Utilities to support interacting with multiple HPC clusters  

Provides the ability to interact with the most popular HPC job scheduling systems using a single interface 
and enables the creation of job submission scripts. The system also tracks statistics of each job and records
the hardware details of the compute nodes used.
 
Data is stored in a local database in ~/.mycluster

Tested with SGE and slurm (LSF and PBS/TORQUE support under development)

Installation - pip install MyCluster or git clone https://github.com/zenotech/MyCluster.git 

Dependencies: ZODB, Fabric

Example usage

Register details
```
mycluster --firstname Fred --lastname Bloggs --email fred.bloggs@email.com
```
List all queues
```
mycluster -q
```
Create job script
```
mycluster --create JOB_SCRIPT --jobqueue QUEUE --script SCRIPT --ntasks=TASKS --jobname=JOB_NAME 
          --project ACCOUNT_NAME --maxtime 12:00:00
```
Submit job
```
mycluster --submit JOB_SCRIPT
```
Delete job
```
mycluster --delete JOB_ID
```
Print job table
```
mycluster -p
```
Print help
```
mycluster --help
```
The SCRIPT to be executed by the JOB_SCRIPT can make use of the following predefined environment variables
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

In order to capture the relevant information it is recommended that the SCRIPT also exports the following
environment variables

```bash
# Application name
export MYCLUSTER_APP_NAME=
# Data size that typifies application performance for this job (e.g number of points or number of cells)
export MYCLUSTER_APP_DATA=
```
