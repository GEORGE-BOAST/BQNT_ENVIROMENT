import logging
from collections import defaultdict, deque
from enum import Enum, unique
from statistics import mean, stdev
from typing import Callable, Deque, Dict, List, Optional, Set, Type

from numpy import isin, nan
from pandas import DataFrame

from bqmonitor.call import Call
from bqmonitor.job import Job
from bqmonitor.payload import ErrorResponse, Payload
from bqmonitor.storage import AbstractStore, NoopStore
from bqmonitor.types import Date, JSONSerializableValue

_logger = logging.getLogger(__name__)


def _record_only_method(f: Callable) -> Callable:
    """
    Description
    -----------
    A decorator defining methods in the Monitor which can only be called when
    recording. If called otherwise, we log a warning and go back to recording mode.
    """

    def _internal(self: "Monitor", *args, **kwargs):
        if isinstance(self._storage_client, NoopStore):
            return
        if self.mode() is Mode.REPLAY:
            _logger.warning(
                (
                    "Attempting to record data while replaying jobs: %s.",
                    "Abandoning replay and continuing current recording",
                ),
                ",".join(self._replay_jobs.keys()),
            )
            self.record()
        return f(self, *args, **kwargs)

    return _internal


@unique
class Mode(Enum):
    """
    The mode defined for the state of the monitor

    - RECORD: The monitor is saving outgoing calls to storage
    - REPLAY: The monitor is fetching and returning saved calls from storage
    """

    RECORD = "RECORD"
    REPLAY = "REPLAY"


class ReplayError(Exception):
    """
    Description
    -----------
    Generic error for failure to replay
    """


class ReplayKeyNotFoundError(ReplayError):
    """
    Description
    -----------
    Error for a replay request not being found in the replay store for a given
    replay key
    """


class Monitor:
    """
    Description
    -----------
    Control interface for interacing with monitor data storage via two modes,
    recording and replaying. In mode record, we save any published calls to the
    storage client. In mode replay, we fetch any previously saved calls from the
    storage client.
    """

    __DEFAULT_STORAGE_CLIENT: Type[AbstractStore] = NoopStore

    def __init__(
        self, storage_client: AbstractStore = __DEFAULT_STORAGE_CLIENT()
    ) -> None:
        # general state
        # ------------
        self._storage_client: AbstractStore = storage_client
        self._should_store_call_body: bool = True
        self._replay_mode: Mode = Mode.RECORD

        # record state
        # ------------
        # make an initial job for the monitor to record from. This is only
        # saved to storage upon the first call being added to the job so
        # we never have empty jobs in storage
        initial_job = Job()
        # a dict ordered by job start time where the key is the job id
        # and the value is the associated job
        self._jobs: Dict[str, Job] = {initial_job.job_id: initial_job}
        # a set of all services from calls coming into bqmonitor
        self._services: Set[str] = set()

        # replay state
        # ------------
        # if we're in replay mode, this will be a dictionary where the key is
        # the request and the value is the response to replay for the given
        # request. If there is more than one call for the given request we will
        # replay them in the order which they were recieved
        self._replay_store: Dict[str, Deque[Call]] = defaultdict(deque)
        # if we're in replay mode, this will be the dict of jobs currently
        # being replayed ordered by job start time where the key is the job
        # id and the value is the associated job
        self._replay_jobs: Dict[str, Job] = {}
        self._replay_services: Set[str] = set()
        # the last call the replayer returned. This is used to know what current
        # stage or job we're on for the backtest
        self._replayer_last_call: Optional[Call] = None

    @property
    def storage_client(self) -> AbstractStore:
        """
        Description
        -----------
        Returns the current storage client being used by the monitor
        """
        return self._storage_client

    @storage_client.setter
    def storage_client(self, storage_client: Optional[AbstractStore]) -> None:
        if self.mode() is Mode.REPLAY:
            logging.warning(
                (
                    "Cannot set a new storage client while replaying."
                    "Setting current mode back to record"
                )
            )
            self.record()
        # reset the state of the monitor by setting a new current job. We don't
        # want to reference our job from the old storage client
        new_initial_job = Job()
        self._jobs = {new_initial_job.job_id: new_initial_job}
        self._services = set()
        # reset the internal state
        self._storage_client = storage_client or NoopStore()

    def services(self) -> List[str]:
        """
        Returns
        -------
        List[str] -
            The names of all services which have published calls to the monitor
            from the current storage client
        """
        return list(
            self._services
            if self._replay_mode is Mode.RECORD
            else self._replay_services
        )

    @property
    def jobs(self) -> List[Job]:
        """
        Description
        -----------
        Returns the jobs being replayed if in replay mode ordered by creation
        time, otherwise returns the jobs which have been created in the
        current session
        """
        return list(
            self._jobs.values()
            if self._replay_mode is Mode.RECORD
            else self._replay_jobs.values()
        )

    @property
    def current_job(self) -> Job:
        """
        Description
        -----------
        Returns the current job being replayed if in replay mode, otherwise
        returns the current job being created
        """
        if self._replay_mode is Mode.RECORD:
            return self.jobs[-1]
        if (
            self._replayer_last_call is not None
            and self._replayer_last_call.job_id is not None
            and self._replayer_last_call.job_id in self._replay_jobs
        ):
            return self._replay_jobs[self._replayer_last_call.job_id]
        raise ReplayError(
            "No current job because the backtest has not started replaying"
        )

    @property
    def stages(self) -> List[str]:
        """
        Description
        -----------
        Returns the stages being replayed if in replay mode, otherwise returns
        the stages which have been created in the current job
        """
        return self.current_job.stages

    @property
    def current_stage(self) -> str:
        """
        Description
        -----------
        Returns the stages being replayed if in replay mode, otherwise returns
        the stages which have been created in the current job
        """
        return self.current_job.stages[-1]

    @_record_only_method
    def publish(self, call: Call) -> None:
        """
        Description
        -----------
        Publishes a given call to the current data store

        Parameters
        ----------
        call : Call
             the call to publish
        """
        try:
            # decorate call with the current job and stage
            call.job_id = self.current_job.job_id
            call.stage = self.current_job.stages[-1]

            # add call to our current job
            self.current_job.add(call.call_id)

            # add call's service to our services
            self._services.add(call.service_name)

            # update storage with the new call and job
            self._storage_client.store_job(self.current_job)
            self._storage_client.store_call(call)
        except TypeError as e:
            _logger.warning(
                "Failed to save response: %s for call %s to storage on job %s because: %s",
                str(call.response),
                call.call_id,
                str(call.job_id),
                str(e),
            )

    @_record_only_method
    def start_job(self, job: Optional[Job] = None) -> None:
        """
        Description
        -----------
        Given a new job, set that job as our current job

        Parameters
        ---------
        job : Job
            the job to set

        Properties
        ----------
        Record only method
        """
        new_job: Job = job or Job()
        # if our current job has no calls, remove it from memory
        if not self.current_job.call_ids:
            del self._jobs[self.current_job.job_id]
        self._jobs[new_job.job_id] = new_job

    @_record_only_method
    def set_stage(self, stage: str) -> None:
        """
        Description
        -----------
        Given a new stage, set that stage as our current stage in our current job

        Parameters
        ---------
        stage : str
            the stage to set

        Properties
        ----------
        Record only method
        """
        self.current_job.stages.append(stage)

    def mode(self) -> Mode:
        """
        Description
        -----------
        Get the current mode of our Monitor

        Returns
        -------
        Mode -
            An enum of either RECORD or REPLAY
        """
        return self._replay_mode

    def record(self) -> None:
        """
        Description
        -----------
        Set our monitor to record incoming requests to the replay store
        """
        self._replay_mode: Mode = Mode.RECORD
        # reset replaying state
        self._replay_jobs: Dict[str, Job] = {}
        self._replay_store: Dict[str, Deque[Call]] = defaultdict(deque)
        self._replayer_last_call: Optional[Call] = None
        self._replay_services: Set[str] = set()

    def replay(self, *job_ids: str) -> None:
        """
        Description
        -----------
        Set our monitor to replay incoming requests from the replay store
        """
        # first flush the replay state, so we dont overpopulate our replay store
        if self.mode() is Mode.REPLAY:
            if any(self._replay_store.values()):
                logging.warning(
                    "Replay called before last replay finished, restarting replay"
                )
            self.record()
        self._replay_mode: Mode = Mode.REPLAY
        # get all the jobs to replay from storage
        if job_ids:
            self._replay_jobs = {
                job_id: self._storage_client.get_job(job_id)
                for job_id in job_ids
            }
        # if no job ids were given, we replay all jobs currently in
        # memory as populated by the current session
        else:
            self._replay_jobs = dict(self._jobs)

        # for each job in our jobs to replay, we populate our replay
        # store and our service store with all the calls for the job
        for call in self.calls():
            if call.replay_key:
                self._replay_store[call.replay_key].appendleft(call)
                self._replay_services.add(call.service_name)

    def replay_request(self, replay_key: str) -> JSONSerializableValue:
        """
        Description
        -----------
        Given a replay key, get that request from storage and return its response

        Parameters
        ---------
        replay_key : str
            the unique key which can be used to access its corrisponding request
            in the replay store

            the replay key is defined in MonitorClientWrapper.replay_key_for_body
            for any inherited monitor client

        Returns
        -------
        JSONSerializableValue -
            Any value which can be serialized to JSON

        Raises
        ------
        ReplayNotFoundError -
            If the replay_key was not found in the replay store

        ReplayError -
            If the Monitor is not in replay mode

            If the Monitor has replayed the request too many times

        Exception -
            Any exception which was raised as a result of the original request
        """
        if self._replay_mode is Mode.RECORD:
            raise ReplayError(
                "Monitor not in replay mode. run",
                "bqmontior.replay() to enable replay mode",
            )
        if replay_key not in self._replay_store:
            raise ReplayKeyNotFoundError(
                f"Request {replay_key} not found in replayed jobs: ",
                f"{','.join(self._replay_jobs.keys())}",
            )
        if not self._replay_store[replay_key]:
            raise ReplayError(
                (
                    f"Request {replay_key} was not replayed this many times in the"
                    " original recording. To replay this request again, restart"
                    " this replay."
                )
            )
        replay_call: Call = self._replay_store[replay_key].pop()
        if not replay_call.response:
            raise ReplayError(
                f"The promise from {replay_call.service_name}"
                f"for {replay_key} was never resolved for replayed job: ",
                str(replay_call.job_id),
            )
        # if at this point in the backtest we got an error, raise that error
        if isinstance(replay_call.response.body, ErrorResponse):
            raise replay_call.response.body.as_exception()
        self._replayer_last_call = replay_call
        return replay_call.response.body

    def requests(
        self,
        job_id: Optional[str] = None,
        stage: Optional[str] = None,
        service: Optional[str] = None,
    ) -> List[Payload]:
        """
        Parameters
        ----------
        job_id : Optional[str] default=None
            optionally filter by the job_id of assoceated requests
             If None, requests from all jobs in memory are returned
        stage : Optional[str] default=None
            optionally filter by the stage of assoceated calls
             If None, all stages from all jobs in memory are returned
        service : Optional[str] default=None
             optionally filter by one of the services publishing calls.
             If None, requests to all services are returned

        Returns
        -------
        List[Payload] -
            The request payload objects from one or all services, jobs, and stages
        """
        return [call.request for call in self.calls(job_id, stage, service)]

    def responses(
        self,
        job_id: Optional[str] = None,
        stage: Optional[str] = None,
        service: Optional[str] = None,
    ) -> List[Payload]:
        """
        Parameters
        ----------
        job_id : Optional[str] default=None
            optionally filter by the job_id of assoceated responses
                If None, responses from all jobs in memory are returned
        stage : Optional[str] default=None
            optionally filter by the stage of assoceated responses
                If None, all responses from all jobs in memory are returned
        service : Optional[str] default=None
                optionally filter by one of the services publishing calls.
                If None, responses to all services are returned

        Returns
        -------
        List[Payload] -
            The response payload objects from one or all services, jobs, and stages
        """
        return [
            call.response
            for call in self.calls(job_id, stage, service)
            if call.response
        ]

    def calls(
        self,
        job_id: Optional[str] = None,
        stage: Optional[str] = None,
        service: Optional[str] = None,
    ) -> List[Call]:
        """
        Parameters
        ----------
        job_id : Optional[str] default=None
            optionally filter by the job_id of assoceated calls
                If None, calls from all jobs in memory are returned
        stage : Optional[str] default=None
            optionally filter by the stage of assoceated calls
                If None, all calls from all jobs in memory are returned
        service : Optional[str] default=None
                optionally filter by one of the services publishing calls.
                If None, calls to all services are returned

        Returns
        -------
        List[Call] -
            The call objects from one or all services, jobs, and stages
        """

        def should_return_call(call: Call) -> bool:
            """
            Description
            -----------
            Helper method to determine if for a given call we should return it based
            on the specified filtering characteristics passed by a user
            """
            ret: bool = True
            if job_id:
                ret &= call.job_id == job_id
            if stage:
                ret &= call.stage == job_id
            if service:
                ret &= call.service_name == service
            return ret

        calls: List[Call] = []
        for job in self.jobs:
            calls += self._storage_client.get_calls(job)
        return [call for call in calls if should_return_call(call)]

    def summary(self, job_id: Optional[str] = None) -> DataFrame:
        """
        Parameters
        ----------
        job_id : Optional[str] default=None
            optionally base the summary on one job by giving the job_id of assoceated
            calls. If None, all jobs in memory are used to build the summary
        Returns
        -------
        pd.DataFrame -
            A summary DataFrame detailing recorded calls and metadata

            columns: num_calls, avg_time(seconds), total_time(seconds),
            max_time(seconds), min_time(seconds), std_dev, avg_request_size(bytes),
            avg_response_size(bytes)

            rows: service(name)
        """
        df = defaultdict(list)

        calls_by_service = {
            service_name: self.calls(job_id=job_id, service=service_name)
            for service_name in self.services()
        }
        for name, calls in calls_by_service.items():
            durations = [c.duration for c in calls]
            df["service"].append(name)
            df["num_calls"].append(len(calls))
            df["avg_time"].append(mean(durations))
            df["total_time"].append(sum(durations))
            df["max_time"].append(max(durations))
            df["min_time"].append(min(durations))
            df["std_dev"].append(
                stdev(durations) if len(durations) >= 2 else nan
            )
        if not df:
            return DataFrame()
        return DataFrame(data=df).set_index("service")
