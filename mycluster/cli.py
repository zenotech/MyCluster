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
import errno
import click
import pyfiglet
from configparser import ConfigParser

import mycluster
from .exceptions import ConfigurationException, SchedulerException

APP_NAME = "MyCluster"


def read_config():
    cfg = os.path.join(click.get_app_dir(APP_NAME), "config.ini")
    if os.path.isfile(cfg):
        parser = ConfigParser(allow_no_value=True)
        parser.read([cfg])
        email = parser.get("default", "email")
        return {"email": email, "silent": True}
    return {"email": "None", "silent": True}


@click.group()
@click.pass_context
@click.option("-s", "--silent", is_flag=True, help="Hide application banner")
@click.option(
    "--email",
    type=str,
    help="Email address to use in submission files",
)
def main(ctx, silent, email):
    """CLI for MyCluster"""
    config = read_config()
    if email is not None:
        config["email"] = email
    if not silent:
        config["silent"] = False
        click.echo(pyfiglet.Figlet().renderText("MyCluster"))
        click.echo(f"User email: '{config['email']}'")
    try:
        scheduler = mycluster.detect_scheduling_sys()
        if not silent:
            click.echo(f"Scheduler '{scheduler.scheduler_type()}' detected")
        ctx.obj = (scheduler, config)
    except ConfigurationException as e:
        click.echo(e)
        exit(1)


@main.command()
@click.pass_context
def configure(ctx):
    """Configure MyCluster to save your email address"""
    email = click.prompt("Please enter your email address")
    config = ConfigParser()
    if not config.has_section("default"):
        config.add_section("default")
    config.set("default", "email", email)
    cfg = os.path.join(click.get_app_dir(APP_NAME), "config.ini")
    try:
        os.makedirs(os.path.dirname(cfg))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open(cfg, "w") as configfile:
        config.write(configfile)
        click.echo(f"Config file written to {cfg}")


@main.command()
@click.pass_context
def queues(ctx):
    """List all available queues"""
    click.echo(
        "{0:25} | {1:^15} | {2:^15} | {3:^15} | {4:^15} | {5:^15}".format(
            "Queue Name",
            "Node Max Task",
            "Node Max Thread",
            "Node Max Memory",
            "Max Task",
            "Available Task",
        )
    )
    for q in ctx.obj[0].queues():
        try:
            nc = ctx.obj[0].node_config(q)
            tpn = ctx.obj[0].tasks_per_node(q)
            avail = ctx.obj[0].available_tasks(q)
        except SchedulerException:
            nc = None
            tpn = None
            avail = None
        click.echo(
            f'{q:25} | {tpn:^15} | {nc["max thread"]:^15} | {nc["max memory"]:^15} | {nc["max task"]:^15} | {avail["available"]:^15}'
        )


@click.argument("runscript")
@click.argument("queue")
@click.argument("jobfile")
@click.option(
    "-j",
    "--jobname",
    default="myclusterjob",
    help="Name to give job",
    show_default=True,
)
@click.option("-n", "--ntasks", default=1, help="Number of tasks", show_default=True)
@click.option(
    "-tpt",
    "--threadspertask",
    type=int,
    required=False,
    help="Number of threads per task",
)
@click.option(
    "-tpn",
    "--taskpernode",
    type=int,
    required=False,
    help="Task per node",
)
@click.option(
    "-p",
    "--project",
    default="default",
    help="Project/Account used of accounting usage",
    show_default=True,
)
@click.option(
    "-q",
    "--qos",
    default="default",
    help="QOS level to submit to",
    show_default=True,
)
@click.option(
    "--ompiargs",
    default="-bysocket -bind-to-socket",
    help="Arguments for OpenMPI",
    show_default=True,
)
@click.option(
    "--maxtime",
    default="12:00:00",
    help="Maximum runtime (hh:mm::ss)",
    show_default=True,
)
@click.option(
    "--shared",
    is_flag=True,
    help="Request shared node allocation",
)
@main.command()
@click.pass_context
def create(
    ctx,
    runscript,
    queue,
    jobfile,
    jobname,
    ntasks,
    threadspertask,
    taskpernode,
    project,
    qos,
    ompiargs,
    maxtime,
    shared,
):
    """Create a job file to submit RUNSCRIPT to QUEUE and write it to JOBFILE"""
    if "email" in ctx.obj[1]:
        user_email = ctx.obj[1]["email"]
    else:
        user_email = None
    script = ctx.obj[0].create_submit(
        queue,
        ntasks,
        jobname,
        runscript,
        maxtime,
        ompiargs,
        project,
        tasks_per_node=taskpernode,
        threads_per_task=threadspertask,
        user_email=user_email,
        qos=qos,
        exclusive=not shared,
    )
    with click.open_file(jobfile, "w") as f:
        f.write(script)
    click.echo(f"Job file written to {jobfile}")


@click.argument("jobfile")
@click.option(
    "--immediate",
    is_flag=True,
    help="Submit job immediately",
)
@click.option(
    "--depends",
    type=str,
    help="List of job dependencies jobA_id:jobB_id:jobC_id (Slurm only)",
)
@main.command()
@click.pass_context
def submit(ctx, jobfile, immediate, depends):
    """Submit a batch script JOBFILE to the scheduler"""
    if os.path.isfile(jobfile):
        job_id = ctx.obj[0].submit(jobfile, immediate, depends)
        click.echo(f"Job submitted with ID '{job_id}'")
    else:
        click.echo(f"Error: Job file '{jobfile}' not found.")


@main.command()
@click.pass_context
def list(ctx):
    """List the status of active jobs"""
    print(
        "{0:^10} | {1:^10} | {2:^10} | {3:^12}".format(
            "Job ID", "Job Name", "Queue", "Status"
        )
    )
    for job in ctx.obj[0].list_current_jobs():
        print(
            f"{job['id']:^10} | {job['name']:^10} | {job['queue']:^10} | {job['state']:^12}"
        )


@click.argument("jobid", type=int)
@main.command()
@click.pass_context
def details(ctx, jobid):
    """Get the details of job with id JOBID"""
    try:
        click.echo(ctx.obj[0].get_job_details(jobid))
    except SchedulerException as e:
        click.echo(e)


@click.argument("jobid", type=int)
@main.command()
@click.pass_context
def cancel(ctx, jobid):
    """Cancel job with id JOBID"""
    try:
        click.echo(ctx.obj[0].delete(jobid))
    except SchedulerException as e:
        click.echo(e)


if __name__ == "__main__":
    main()
