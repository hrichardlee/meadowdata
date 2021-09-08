import pickle
from typing import Iterable

from nextbeat.event_log import Event, AppendEventType
from nextbeat.jobs_common import JobPayload, JobRunner, RaisedException
from nextrun.client import NextRunClientAsync, ProcessStateEnum
from nextrun.config import DEFAULT_ADDRESS
from nextrun.job_run_spec import (
    JobRunSpecFunction,
    JobRunSpecDeployedFunction,
    JobRunSpec,
    convert_function_to_deployed_function,
)


class NextRunJobRunner(JobRunner):
    """Integrates nextrun with nextbeat. Runs jobs on a nextrun server."""

    def __init__(self, append_event: AppendEventType, address: str = DEFAULT_ADDRESS):
        self._client = NextRunClientAsync(address)
        self._append_event = append_event

    async def _run_deployed_function(
        self,
        job_name: str,
        run_request_id: str,
        job_run_spec: JobRunSpecDeployedFunction,
    ) -> None:
        self._append_event(job_name, JobPayload(run_request_id, "RUN_REQUESTED"))

        result = await self._client.run_py_func(run_request_id, job_run_spec)
        if result.state == ProcessStateEnum.REQUEST_IS_DUPLICATE:
            # TODO handle this case as well as other failures in requesting run
            raise NotImplementedError()
        elif result.state == ProcessStateEnum.RUNNING:
            # TODO there is a very bad race condition here--the sequence of events could
            #  be:
            #  - run records RUN_REQUESTED
            #  - the nextrun server runs the job and it completes
            #  - poll_jobs runs and records SUCCEEDED
            #  - the post-await continuation of run happens and records RUNNING
            self._append_event(
                job_name,
                JobPayload(run_request_id, "RUNNING", pid=result.pid),
            )
        else:
            raise ValueError(f"Did not expect ProcessStateEnum {result.state}")

    async def run(
        self, job_name: str, run_request_id: str, job_run_spec: JobRunSpec
    ) -> None:
        """
        Dispatches to _run_deployed_function which calls nextrun depending on the
        job_run_spec
        """
        if isinstance(job_run_spec, JobRunSpecDeployedFunction):
            await self._run_deployed_function(job_name, run_request_id, job_run_spec)
        elif isinstance(job_run_spec, JobRunSpecFunction):
            await self._run_deployed_function(
                job_name,
                run_request_id,
                convert_function_to_deployed_function(job_run_spec),
            )
        else:
            raise ValueError(
                f"job_run_spec of type {type(job_run_spec)} is not supported by "
                f"NextRunJobRunner"
            )

    async def poll_jobs(self, last_events: Iterable[Event[JobPayload]]) -> None:
        """
        See docstring on base class. This code basically translates the nextrun
        ProcessState into a JobPayload
        """

        last_events = list(last_events)
        process_states = await self._client.get_process_states(
            [e.payload.request_id for e in last_events]
        )

        if len(last_events) != len(process_states):
            raise ValueError(
                "get_process_states returned a different number of requests than "
                f"expected, sent {len(last_events)}, got back {len(process_states)} "
                "responses"
            )

        for last_event, process_state in zip(last_events, process_states):
            request_id = last_event.payload.request_id
            if (
                process_state.state == ProcessStateEnum.UNKNOWN
                or process_state.state == ProcessStateEnum.ERROR_GETTING_STATE
            ):
                # TODO handle this case and test it
                raise NotImplementedError("Not sure what to do here?")
            elif process_state.state == ProcessStateEnum.SUCCEEDED:
                new_payload = JobPayload(
                    request_id,
                    "SUCCEEDED",
                    pid=process_state.pid,
                    # TODO probably handle unpickling errors specially
                    result_value=pickle.loads(process_state.pickled_result),
                )
            elif process_state.state == ProcessStateEnum.PYTHON_EXCEPTION:
                new_payload = JobPayload(
                    request_id,
                    "FAILED",
                    failure_type="PYTHON_EXCEPTION",
                    pid=process_state.pid,
                    # TODO probably handle unpickling errors specially
                    raised_exception=RaisedException(
                        *pickle.loads(process_state.pickled_result)
                    ),
                )
            elif process_state.state == ProcessStateEnum.NON_ZERO_RETURN_CODE:
                # TODO Test this case
                new_payload = JobPayload(
                    request_id,
                    "FAILED",
                    failure_type="NON_ZERO_RETURN_CODE",
                    pid=process_state.pid,
                    return_code=process_state.return_code,
                )
            elif process_state.state == ProcessStateEnum.CANCELLED:
                # TODO handle this and test it
                raise NotImplementedError("TBD")
            elif process_state.state == ProcessStateEnum.RUNNING:
                new_payload = JobPayload(request_id, "RUNNING", pid=process_state.pid)
            else:
                raise ValueError(
                    f"Did not expect ProcessStateEnum {process_state.state}"
                )

            if last_event.payload.state != new_payload.state:
                if (
                    last_event.payload.state == "RUN_REQUESTED"
                    and new_payload.state != "RUNNING"
                ):
                    self._append_event(
                        last_event.topic_name,
                        JobPayload(request_id, "RUNNING", pid=new_payload.pid),
                    )
                self._append_event(last_event.topic_name, new_payload)

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._client.__aexit__(exc_type, exc_val, exc_tb)