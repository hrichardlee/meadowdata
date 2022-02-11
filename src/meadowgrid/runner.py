from __future__ import annotations

import abc
import asyncio
import dataclasses
import io
import itertools
import os.path
import pickle
import shlex
import threading
import uuid
from typing import Callable, TypeVar, Union, Any, Dict, Optional, Sequence, cast

import fabric
import paramiko.ssh_exception

from meadowgrid import ServerAvailableFolder
from meadowgrid.agent import run_one_job
from meadowgrid.aws_integration import (
    _MEADOWGRID_AWS_AMI,
    ensure_meadowgrid_ssh_security_group,
    launch_ec2_instances,
)
from meadowgrid.config import (
    DEFAULT_INTERRUPTION_PROBABILITY_THRESHOLD,
    MEADOWGRID_INTERPRETER,
    DEFAULT_LOGICAL_CPU_REQUIRED,
    DEFAULT_MEMORY_GB_REQUIRED,
)
from meadowgrid.coordinator_client import (
    _add_deployments_to_job,
    _create_py_function,
    _make_valid_job_id,
    _pickle_protocol_for_deployed_interpreter,
    _string_pairs_from_dict,
)
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridFunction,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowgrid.grid import _get_id_name_function
from meadowgrid.meadowgrid_pb2 import (
    JobToRun,
    Job,
    ProcessState,
    PyCommandJob,
    ServerAvailableInterpreter,
)

_T = TypeVar("_T")


def _construct_job_to_run_function(
    function: Callable[..., _T],
    deployment: Optional[Deployment],
    args: Optional[Sequence[Any]],
    kwargs: Optional[Dict[str, Any]],
    ports: Optional[Sequence[str]],
) -> JobToRun:
    """
    Basically a "user-friendly" JobToRun constructor for functions.

    Kind of a combination of meadowgrid.grid.grid_map and
    meadowgrid.coordinator_client._create_py_runnable_job
    """

    job_id, friendly_name, pickled_function = _get_id_name_function(function)

    if deployment is None:
        deployment = Deployment.get_default()
    if deployment.code is None:
        deployment.code = ServerAvailableFolder()

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()
    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(friendly_name),
        environment_variables=_string_pairs_from_dict(deployment.environment_variables),
        ports=ports or (),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_function=_create_py_function(
            MeadowGridFunction.from_pickled(pickled_function, args, kwargs),
            pickle_protocol,
        ),
    )
    _add_deployments_to_job(job, deployment.code, deployment.interpreter)
    return JobToRun(job=job)


def _construct_job_to_run_command(
    args: Union[str, Sequence[str]],
    deployment: Optional[Deployment],
    ports: Optional[Sequence[str]],
) -> JobToRun:
    """Basically a "user-friendly" JobToRun constructor for commands."""

    job_id = str(uuid.uuid4())
    if isinstance(args, str):
        args = shlex.split(args)
    # this is kind of a silly way to get a friendly name--treat the first three
    # elements of args as if they're paths and take the last part of each path
    friendly_name = "-".join(os.path.basename(arg) for arg in args[:3])

    if deployment is None:
        deployment = Deployment.get_default()
    if deployment.code is None:
        deployment.code = ServerAvailableFolder()

    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(friendly_name),
        environment_variables=_string_pairs_from_dict(deployment.environment_variables),
        ports=ports or (),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_command=PyCommandJob(command_line=args),
    )
    _add_deployments_to_job(job, deployment.code, deployment.interpreter)
    return JobToRun(job=job)


@dataclasses.dataclass
class Deployment:
    interpreter: Union[InterpreterDeployment, VersionedInterpreterDeployment]
    code: Union[CodeDeployment, VersionedCodeDeployment, None] = None
    environment_variables: Optional[Dict[str, str]] = None

    @classmethod
    def get_default(cls) -> Deployment:
        return cls(
            interpreter=ServerAvailableInterpreter(
                interpreter_path=MEADOWGRID_INTERPRETER
            )
        )


class Host(abc.ABC):
    @abc.abstractmethod
    async def run_job(self, job_to_run: JobToRun) -> Any:
        pass


@dataclasses.dataclass(frozen=True)
class LocalHost(Host):
    async def run_job(self, job_to_run: JobToRun) -> Any:
        initial_update, continuation = await run_one_job(job_to_run)
        if initial_update.process_state.state != \
                ProcessState.ProcessStateEnum.RUNNING or continuation is None:
            result = initial_update.process_state
        else:
            result = (await continuation).process_state

        if result.state == ProcessState.ProcessStateEnum.SUCCEEDED:
            # TODO figure out what to do about the [0], which is there for dropping effects
            return pickle.loads(result.pickled_result)[0]
        else:
            # TODO make better error messages
            raise ValueError(f"Error: {result.state}")


@dataclasses.dataclass(frozen=True)
class SshHost(Host):
    address: str
    # these options are forwarded directly to Fabric
    fabric_kwargs: Optional[Dict[str, Any]] = None

    async def run_job(self, job_to_run: JobToRun) -> Any:
        with fabric.Connection(
            self.address, **(self.fabric_kwargs or {})
        ) as connection:
            job_io_prefix = ""

            try:
                # assumes that meadowgrid is installed in /meadowgrid/env as per
                # build_meadowgrid_amis.md. Also uses the default working_folder, which
                # should (but doesn't strictly need to) correspond to
                # agent._set_up_working_folder

                # try the first command 3 times, as this is when we actually try to
                # connect to the remote machine.
                home_result = await _retry(
                    lambda: connection.run("echo $HOME"),
                    cast(Exception, paramiko.ssh_exception.NoValidConnectionsError),
                )
                if not home_result.ok:
                    raise ValueError(
                        "Error getting home directory on remote machine "
                        + home_result.stdout
                    )

                remote_working_folder = f"{home_result.stdout.strip()}/meadowgrid"
                mkdir_result = connection.run(f"mkdir -p {remote_working_folder}/io")
                if not mkdir_result.ok:
                    raise ValueError(
                        "Error creating meadowgrid directory " + mkdir_result.stdout
                    )

                job_io_prefix = f"{remote_working_folder}/io/{job_to_run.job.job_id}"

                # serialize job_to_run and send it to the remote machine
                with io.BytesIO(
                    job_to_run.SerializeToString()
                ) as job_to_run_serialized:
                    connection.put(
                        job_to_run_serialized, remote=f"{job_io_prefix}.job_to_run"
                    )

                # fabric doesn't have a way of executing things: https://github.com/fabric/fabric/pull/2010/files
                result_future: asyncio.Future = asyncio.Future()
                event_loop = asyncio.get_running_loop()

                def run_and_wait() -> None:
                    try:
                        # use meadowrun to run the job
                        returned_result = connection.run(
                            "/meadowgrid/env/bin/meadowrun "
                            f"--job-io-prefix {job_io_prefix} "
                            f"--working-folder {remote_working_folder}"
                        )
                        event_loop.call_soon_threadsafe(
                            lambda returned_result=returned_result: result_future.set_result(
                                returned_result
                            )
                        )
                    except Exception as e2:
                        event_loop.call_soon_threadsafe(
                            lambda e2=e2: result_future.set_exception(e2)
                        )

                threading.Thread(target=run_and_wait).start()

                result = await result_future

                # TODO consider using result.tail, result.stdout

                # copied/adapted from meadowgrid.agent._completed_job_state

                # see if we got a normal return code
                if result.return_code != 0:
                    raise ValueError(f"Process exited {result.return_code}")

                with io.BytesIO() as result_buffer:
                    connection.get(f"{job_io_prefix}.process_state", result_buffer)
                    result_buffer.seek(0)
                    process_state = ProcessState()
                    process_state.ParseFromString(result_buffer.read())

                if process_state.state == ProcessState.ProcessStateEnum.SUCCEEDED:
                    job_spec_type = job_to_run.job.WhichOneof("job_spec")
                    # we must have a result from functions, in other cases we can optionally
                    # have a result
                    if job_spec_type == "py_function" or process_state.pickled_result:
                        return pickle.loads(process_state.pickled_result)
                    else:
                        return None
                else:
                    raise ValueError(f"Running remotely failed: {process_state}")
            finally:
                if job_io_prefix:
                    for remote_path in (
                        f"{job_io_prefix}.job_to_run",
                        f"{job_io_prefix}.state",
                        f"{job_io_prefix}.result",
                        f"{job_io_prefix}.process_state",
                    ):
                        try:
                            # -f so that we don't throw an error on files that don't exist
                            connection.run(f"rm -f {remote_path}")
                        except Exception as e:
                            print(
                                f"Error cleaning up files on remote machine: "
                                f"{remote_path} {e}"
                            )

                        # TODO also clean up log file?s


@dataclasses.dataclass(frozen=True)
class NewEC2Host(Host):
    logical_cpu_required: float = DEFAULT_LOGICAL_CPU_REQUIRED
    memory_gb_required: float = DEFAULT_MEMORY_GB_REQUIRED

    interruption_probability_threshold: float = (
        DEFAULT_INTERRUPTION_PROBABILITY_THRESHOLD
    )

    region_name: Optional[str] = None

    security_group_ids: Sequence[str] = ()
    # the name of an SSH key as set up in AWS. If None, the default as per AWS will
    # be used
    ssh_key_name: Optional[str] = None
    # the path to the private key file that corresponds to ssh_key_name. If None,
    # assumes that it's in "the usual place"
    path_to_private_ssh_key: Optional[str] = None

    # TODO add security groups, iam roles

    async def _launch(self) -> SshHost:
        ec2_host = await launch_ec2_instances(
            self.logical_cpu_required,
            self.memory_gb_required,
            1,
            self.interruption_probability_threshold,
            _MEADOWGRID_AWS_AMI,
            self.region_name,
            list(
                itertools.chain(
                    [await ensure_meadowgrid_ssh_security_group()],
                    self.security_group_ids,
                )
            ),
            key_name=self.ssh_key_name,
            wait_for_dns_name=True,
        )

        if len(ec2_host) != 1:
            raise ValueError(f"Asked for 1 task but got back {len(ec2_host)} machines!")

        # where is the user "coming" from??
        return SshHost(
            ec2_host[0].public_dns_name,
            {
                "user": "ubuntu",
                "connect_kwargs": {"key_filename": self.path_to_private_ssh_key},
            },
        )

    async def run_job(self, job_to_run: JobToRun) -> Any:
        return await (await self._launch()).run_job(job_to_run)


async def run_function(
    function: Callable[..., _T],
    host: Host,
    deployment: Optional[Deployment] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    ports: Optional[Sequence[str]] = None,
) -> _T:
    """
    Same as run_function_async, but runs on a remote machine, specified by "host".
    Connects to the remote machine over SSH via the fabric library
    https://www.fabfile.org/ fabric_kwargs are passed directly to fabric.Connection().

    The remote machine must have meadowgrid installed as per build_meadowgrid_amis.md

    ports is just for running containers, e.g. NewEC2Host must have the right
    security groups set separately
    """

    return (
        await host.run_job(
            _construct_job_to_run_function(
                function,
                deployment,
                args,
                kwargs,
                ports,
            )
        )
    )[0]
    # drop effects...


async def run_command(
    args: Union[str, Sequence[str]],
    host: Host,
    deployment: Optional[Deployment] = None,
    ports: Optional[Sequence[str]] = None,
) -> None:
    """
    Runs the specified command on a remote machine. See run_function_remote for more
    details on requirements for the remote host.
    """

    await host.run_job(_construct_job_to_run_command(args, deployment, ports))


async def _retry(
    function: Callable[[], _T],
    exception_types: Exception,
    max_num_attempts: int = 3,
    delay_seconds: float = 1,
) -> _T:
    i = 0
    while True:
        try:
            return function()
        except exception_types as e:  # type: ignore
            i += 1
            if i >= max_num_attempts:
                raise
            else:
                print(f"Retrying on error: {e}")
                await asyncio.sleep(delay_seconds)
