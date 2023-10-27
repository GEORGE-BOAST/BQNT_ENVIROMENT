"""
Description
-----------
A global monitoring service for monitoring and inspecting calls in to and out of
BQuant.

Example
-------
    >>> import bqmonitor
    >>> import bql
    >>> bqmonitor.enable()
    >>> bq = bql.Service()
    >>> bq.execute("get(PX_LAST) for('AAPL US Equity')")
    ...
    >>> # see the first request to bql
    >>> bqmonitor.requests()[0]

    >>> # see the last response from bql
    >>> bqmonitor.responses()[-1]

    >>> # see how long the first call took
    >>> bqmonitor.calls()[0].duration

    >>> # see a summary table of call metadata
    >>> bqmonitor.summary()
"""
import logging
from typing import List, Optional

from pandas import DataFrame

from bqmonitor import monitor_client_wrapper, storage
from bqmonitor.call import Call
from bqmonitor.job import Job
from bqmonitor.monitor import Mode
from bqmonitor.monitor import Mode as modes
from bqmonitor.monitor import Monitor, ReplayError, ReplayKeyNotFoundError
from bqmonitor.payload import Payload
from bqmonitor.storage import AbstractStore
from bqmonitor.storage.file_store import FileStore
from bqmonitor.storage.noop_store import NoopStore
from bqmonitor.types import JSONSerializableValue

_MONITOR = Monitor()

_logger = logging.getLogger(__name__)

__all__ = [
    "Payload",
    "Call",
    "publish",
    "requests",
    "responses",
    "calls",
    "summary",
    "services",
    "average_time",
    "enable_store_call_body",
    "store_call_body",
    "disable_store_call_body",
    "enable",
    "storage",
    "Job",
    "ReplayError",
    "ReplayKeyNotFoundError",
    "get_monitor",
    "monitoring_enabled",
    "disable",
    "modes",
    "monitor_client_wrapper",
]

"""
Replay methods
"""


def mode() -> Mode:
    """
    Description
    -----------
    Get the current mode of bqmonitor

    Returns
    -------
    Mode -
        An enum of either RECORD or REPLAY
    """
    return _MONITOR.mode()


def replay(*job_ids: str) -> None:
    """
    Description
    -----------
    Set bqmonitor to replay incoming requests from the replay store
    """
    return _MONITOR.replay(*job_ids)


def record() -> None:
    """
    Description
    -----------
    Set bqmonitor to record incoming requests to the replay store
    """
    return _MONITOR.record()


def replay_request(replay_key: str) -> JSONSerializableValue:
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
    return _MONITOR.replay_request(replay_key)


"""
Monitor methods
"""


def publish(call: Call) -> None:
    """
    Description
    -----------
    Publishes a given call to the current data store

    Parameters
    ----------
    call : Call
            the call to publish
    """
    return _MONITOR.publish(call)


def requests(
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
    return _MONITOR.requests(job_id, stage, service)


def responses(
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
    return _MONITOR.responses(job_id, stage, service)


def calls(
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
    return _MONITOR.calls(job_id, stage, service)


def summary(job_id: Optional[str] = None) -> DataFrame:
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
    return _MONITOR.summary(job_id)


def services() -> List[str]:
    """
    Returns
    -------
    List[str] -
        The names of all services which have published calls to the monitor
    """
    return _MONITOR.services()


def current_job() -> Job:
    """
    Description
    -----------
    Returns the current job being replayed if in replay mode, otherwise
    returns the current job being created
    """
    return _MONITOR.current_job


def jobs() -> List[Job]:
    """
    Description
    -----------
    Returns the jobs being replayed if in replay mode ordered by creation
    time, otherwise returns the jobs which have been created in the
    current session
    """
    return _MONITOR.jobs


"""
Monitor control methods
"""


def set_stage(stage: str) -> None:
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
    return _MONITOR.set_stage(stage)


def current_stage() -> str:
    """
    Description
    -----------
    Returns the stages being replayed if in replay mode, otherwise returns
    the stages which have been created in the current job
    """
    return _MONITOR.current_stage


def current_job() -> Job:
    """
    Description
    -----------
    Returns the current job being replayed if in replay mode, otherwise
    returns the current job being created
    """
    return _MONITOR.current_job


def start_job(job: Optional[Job] = None) -> None:
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
    return _MONITOR.start_job(job)


def enable(store: Optional[AbstractStore] = None) -> None:
    """
    Description
    -----------
    Enables monitoring with the default storage client. The same as:
    >>> bqmonitor.set_storage(store)
    >>> bqmonitor.record()
    """
    if store is None:
        store = FileStore()

    record()
    return set_storage(store)


def set_storage(storage_client: Optional[AbstractStore]) -> None:
    """
    Description
    -----------
    Replace the current storage client with a new storage client, by default a the NoOpStore

    Parameters
    ----------
    storage_client - Optional[AbstractStore] - The storage client to use
    """
    _MONITOR.storage_client = storage_client


def storage_client() -> AbstractStore:
    """
    Description
    -----------
    Returns the current storage client being used by the monitor
    """
    return _MONITOR.storage_client


def get_monitor() -> Monitor:
    """
    Returns
    -------
    Monitor -
        The underlying monitor singleton currently in use
    """
    return _MONITOR


def monitoring_enabled() -> bool:
    """
    Returns
    -------
    If monitoring is enabled
    """
    return not isinstance(_MONITOR.storage_client, NoopStore)


def disable() -> None:
    """
    Description
    -----------
    Disable monitoring for all monitored services. The same as:
    >>> bqmonitor.set_storage(None)
    """
    record()
    set_storage(None)
