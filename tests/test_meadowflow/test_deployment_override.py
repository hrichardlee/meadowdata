from typing import Union

from meadowflow.jobs import Job, JobRunOverrides
from meadowflow.meadowgrid_job_runner import MeadowGridJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.topic_names import pname
import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.deployed_function import (
    CodeDeployment,
    GitRepo,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
    VersionedCodeDeployment,
)
from meadowgrid.meadowgrid_pb2 import (
    GitRepoCommit,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)
from test_meadowflow.test_scheduler import _wait_for_scheduler
from test_meadowgrid.test_meadowgrid_basics import (
    EXAMPLE_CODE,
    MEADOWDATA_CODE,
    TEST_REPO,
    TEST_WORKING_FOLDER,
)


def test_deployment_override() -> None:
    """Tests using JobRunOverride.deployment"""
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(MeadowGridJobRunner)

            s.add_jobs(
                [
                    Job(
                        pname("A"),
                        MeadowGridDeployedRunnable(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                            ),
                            ServerAvailableInterpreter(
                                interpreter_path=MEADOWGRID_INTERPRETER
                            ),
                            MeadowGridFunction.from_name(
                                "example_package.example", "unique_per_deployment"
                            ),
                        ),
                        (),
                    )
                ]
            )

            s.main_loop()

            expected_num_events = 1

            def result_with_deployment(
                deployment: Union[CodeDeployment, VersionedCodeDeployment]
            ) -> str:
                """
                Runs the A job with the specified deployment override and returns the
                result_value
                """
                nonlocal expected_num_events

                s.manual_run(pname("A"), JobRunOverrides(code_deployment=deployment))
                _wait_for_scheduler(s)
                events = s.events_of(pname("A"))
                expected_num_events += 3
                assert len(events) == expected_num_events
                assert events[0].payload.state == "SUCCEEDED"
                return events[0].payload.result_value

            # same as original
            result = result_with_deployment(
                ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE])
            )
            assert result == "embedded in main repo"

            # specific commit
            result = result_with_deployment(
                GitRepoCommit(
                    repo_url=TEST_REPO,
                    commit="2fcaca67ea40c35a96de39716e32e4c74cb7f221",
                )
            )
            assert result == "in test_repo older commit"

            # branch
            result = result_with_deployment(GitRepo(TEST_REPO, "test_branch"))
            assert result == "in test_repo test_branch"

            # a different branch
            result = result_with_deployment(GitRepo(TEST_REPO, "main"))
            assert result == "in test_repo newer commit"