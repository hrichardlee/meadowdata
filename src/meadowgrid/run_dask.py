import asyncio
import dataclasses
import itertools
import threading
from typing import Optional, Sequence, Tuple, Awaitable

import psutil

from meadowgrid.aws_integration import (
    ensure_security_group,
    _get_current_ip_for_ssh,
    launch_ec2_instances,
    _MEADOWGRID_AWS_AMI,
    ensure_meadowgrid_ssh_security_group, EC2Host,
)
from meadowgrid.runner import (
    Deployment,
    Host,
    SshHost,
    run_function,
    NewEC2Host,
    run_command,
)

import dask.distributed

_DASK_SCHEDULER_SECURITY_GROUP = "DASK_SCHEDULER_SECURITY_GROUP"
_DASK_WORKER_SECURITY_GROUP = "DASK_WORKER_SECURITY_GROUP"
_DASK_WORKER_PORT_FROM = 8800
_DASK_WORKER_PORT_TO = 8900


async def ensure_ec2_dask_security_groups() -> Tuple[str, str]:
    """Returns scheduler security group id, worker security group id"""
    current_ip = await _get_current_ip_for_ssh()

    worker_security_group_id = ensure_security_group(
        _DASK_WORKER_SECURITY_GROUP, [], []
    )

    scheduler_security_group_id = ensure_security_group(
        _DASK_SCHEDULER_SECURITY_GROUP,
        [(8786, 8787, f"{current_ip}/32")],
        [(8786, 8786, worker_security_group_id)],
    )

    worker_security_group_id2 = ensure_security_group(
        _DASK_WORKER_SECURITY_GROUP,
        [],
        [(_DASK_WORKER_PORT_FROM, _DASK_WORKER_PORT_TO, scheduler_security_group_id)],
    )
    assert worker_security_group_id == worker_security_group_id2

    return scheduler_security_group_id, worker_security_group_id


def run_dask_remote(
    scheduler_host: Host,
    existing_worker_hosts: Sequence[SshHost] = (),
    num_new_ec2_hosts: int = 0,
    ec2_host_spec: Optional[NewEC2Host] = None,
    deployment: Optional[Deployment] = None,
) -> dask.distributed.Client:

    event = threading.Event()
    client: Optional[dask.distributed.Client] = None

    async def permanent_thread() -> None:
        nonlocal client
        client, remainder = await run_dask_remote_async(
            scheduler_host,
            existing_worker_hosts,
            num_new_ec2_hosts,
            ec2_host_spec,
            deployment,
        )
        event.set()
        await remainder

    # this is awful
    threading.Thread(
        target=lambda: asyncio.run(permanent_thread()), daemon=True
    ).start()

    event.wait()
    assert client is not None
    return client


async def run_dask_remote_async(
    scheduler_host: Host,
    existing_worker_hosts: Sequence[SshHost] = (),
    num_new_ec2_hosts: int = 0,
    ec2_host_spec: Optional[NewEC2Host] = None,
    deployment: Optional[Deployment] = None,
) -> Tuple[dask.distributed.Client, Awaitable]:

    (
        scheduler_security_group_id,
        worker_security_group_id,
    ) = await ensure_ec2_dask_security_groups()

    # very hacky, but not sure how else to deal with this
    if isinstance(scheduler_host, NewEC2Host):
        scheduler_host = dataclasses.replace(
            scheduler_host,
            security_group_ids=list(
                itertools.chain(
                    scheduler_host.security_group_ids, [scheduler_security_group_id]
                )
            ),
        )

        scheduler_host = await scheduler_host._launch()

    scheduler_address = getattr(scheduler_host, "address")
    if not scheduler_address:
        raise ValueError(
            f"Didn't know how to get an address from {type(scheduler_host)}"
        )

    scheduler_task = asyncio.create_task(
        run_command(
            "dask-scheduler --host 0.0.0.0",
            scheduler_host,
            deployment,
            ["8786-8787"],
        )
    )

    # new section

    existing_cpu_counts_task = asyncio.gather(
        *[
            run_function(psutil.cpu_count, existing_host)
            for existing_host in existing_worker_hosts
        ]
    )

    if ec2_host_spec is None:
        # probably all defaults is not what we want...
        ec2_host_spec = NewEC2Host()

    if num_new_ec2_hosts > 0:

        new_ec2_hosts_task: Awaitable[Sequence[EC2Host]] = launch_ec2_instances(
            ec2_host_spec.logical_cpu_required,
            ec2_host_spec.memory_gb_required,
            num_new_ec2_hosts,
            ec2_host_spec.interruption_probability_threshold,
            _MEADOWGRID_AWS_AMI,
            ec2_host_spec.region_name,
            [await ensure_meadowgrid_ssh_security_group(), worker_security_group_id],
            key_name=ec2_host_spec.ssh_key_name,
        )
    else:
        new_ec2_hosts_future: asyncio.Future = asyncio.Future()
        new_ec2_hosts_future.set_result([])

        new_ec2_hosts_task = new_ec2_hosts_future

    existing_cpu_counts, new_ec2_hosts = await asyncio.gather(
        existing_cpu_counts_task, new_ec2_hosts_task
    )

    # now wait for the scheduler to be available
    print("getting client")
    result = dask.distributed.Client(f"{scheduler_address}:8786", timeout=30)
    print("got client")

    worker_tasks = []

    for existing_host, cpu_count in zip(existing_worker_hosts, existing_cpu_counts):
        for i in range(cpu_count):
            port = str(8800 + i)
            worker_tasks.append(
                asyncio.create_task(
                    run_command(
                        "dask-worker --contact-address "
                        f"tcp://{existing_host.address}:{port} "
                        f"--listen-address tcp://0.0.0.0:{port} "
                        "--nthreads 1 "
                        f"{scheduler_address}:8786",
                        existing_host,
                        deployment,
                        [port],
                    )
                )
            )
        # nproc is not available

    for new_ec2_host in new_ec2_hosts:
        for i in range(new_ec2_host.logical_cpus):
            port = str(8800 + i)
            print(f"about to launch worker {new_ec2_host.public_dns_name}")
            worker_tasks.append(
                asyncio.create_task(
                    run_command(
                        "dask-worker --contact-address "
                        f"tcp://{new_ec2_host.public_dns_name}:{port} "
                        f"--listen-address tcp://0.0.0.0:{port} "
                        "--nthreads 1 "
                        f"{scheduler_address}:8786",
                        SshHost(
                            new_ec2_host.public_dns_name,
                            fabric_kwargs={
                                "user": "ubuntu",
                                "connect_kwargs": {
                                    "key_filename": ec2_host_spec.path_to_private_ssh_key
                                },
                            },
                        ),
                        deployment,
                        [port],
                    )
                )
            )

    if not worker_tasks:
        raise ValueError("No worker tasks specified!")

    return result, asyncio.gather(scheduler_task, *worker_tasks)
