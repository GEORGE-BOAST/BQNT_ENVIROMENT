import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List

from bqmonitor.call import Call
from bqmonitor.job import Job
from bqmonitor.storage.abstract_store import AbstractStore, StorageError

_logger = logging.getLogger(__name__)


class MemoryStore(AbstractStore):
    """
    Description
    -----------
    In memory storage client for jobs and calls

    Methods
    -------
    store_job - Stores job into the MemoryStore

    get_job - Retrieve stored job from the MemoryStore. If job doesn't exist, raises a StorageError

    store_call - Store call into the MemoryStore

    get_calls - Retrieve all calls belonging to a job from the MemoryStore

    """

    def __init__(self) -> None:
        # a dict where key is the job_id and value is the associated
        # Job object
        self.__jobs_map: Dict[str, Job] = {}

        # a dict where key is the job_id and value is a dict where key
        # being call_id and value being associated Call object
        self.__calls_map: Dict[str, Dict[str, Call]] = defaultdict(dict)

    def store_job(self, job: Job) -> None:
        """
        Description
        -----------
        Store job in the MemoryStore.

        Parameters
        ----------
        job: Job
             Job to store

        Raises
        ------
        ValueError :
            If the job_id of job passed in is None.

        Returns
        -------
        None
        """

        _logger.info("storing in memory storage client job:", job)

        if job.job_id is None:
            msg = f"job_id is none for {job}, cannot store the job"
            raise ValueError(msg)

        self.__jobs_map[job.job_id] = job

    def get_job(self, job_id: str) -> Job:
        """
        Description
        -----------
        Retreive job stored in MemoryStore

        Parameters
        ----------
        job_id : str
                 Unique ID of job to be retrieved

        Raises
        ------
        StorageError :
            If the job was not found for the given job_id


        Returns
        -------
        Job :
              The Job with the given job_id

        """

        _logger.info("getting from memory storage client job:", job_id)

        if job_id in self.__jobs_map:
            return self.__jobs_map[job_id]
        else:
            msg = f"error getting job with id {job_id}, the job_id does not exist"
            raise StorageError(msg)

    def store_call(self, call: Call) -> None:
        """
        Description
        -----------
        Store call in MemoryStore

        Parameters
        ----------
        call : Call
               Call to store

        Raises
        ------
        ValueError :
            If the call does not contain job_id or stage or replay_key

        Returns
        -------
        None
        """
        if not call.job_id:
            raise ValueError("Expected call to have job but job_id is None")
        if not call.stage:
            raise ValueError("Expected call to have stage but stage is None")
        if not call.replay_key:
            raise ValueError(
                "Expected call to have replay_key but replay_key is None"
            )

        _logger.info(f"storing job: {call.job_id} call: {call.call_id}")

        self.__calls_map[call.job_id][call.call_id] = call

    def get_calls(self, job: Job) -> List[Call]:
        """
        Description
        -----------
        Retreive list of calls for given job_id

        Parameters
        ----------
        job_id : str
                 Unique ID of job whose calls are to be retrieved

        Raises
        ------
        StorageError :
            If the job_id of job passed in is None.

        Returns
        -------
        List[Call]
        """

        _logger.info("Gettings calls from memory storage client for job:", job)

        if job.job_id is None:
            msg = f"error getting calls for job {job}, the job_id does not exist for this job"
            raise StorageError(msg)

        return list(self.__calls_map[job.job_id].values())
