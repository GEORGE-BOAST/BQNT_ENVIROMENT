from abc import ABC, abstractmethod
from typing import List

from bqmonitor.call import Call
from bqmonitor.job import Job


class StorageError(Exception):
    """
    Description
    -----------
    An all purpose exception for any known errors saving to or retrieving
    from storage.
    """


class AbstractStore(ABC):
    """
    Description
    -----------
    An interface for implementing data stores for bqmonitor.
    """

    @abstractmethod
    def store_job(self, job: Job) -> None:
        """
        Description
        -----------
        Stores a given bqmonitor.Job in storage

        Parameters
        ----------
        job : Job
            The job to store
        """

    @abstractmethod
    def get_job(self, job_id: str) -> Job:
        """
        Description
        -----------
        Gets a previously stored job from storage given the job_id

        Parameters
        ----------
        job_id : str
            The id of the job to retrieve from storage

        Raises
        ------
        StorageError :
            If the job was not found for the given job_id
        """

    @abstractmethod
    def store_call(self, call: Call) -> None:
        """
        Description
        -----------
        Stores a given bqmonitor.Call in storage

        Parameters
        ----------
        call : Call
            The call to store
        """

    @abstractmethod
    def get_calls(self, job: Job) -> List[Call]:
        """
        Description
        -----------
        Gets all previously stored calls from storage given the job_id assoceated
        with all of said calls.

        Parameters
        ----------
        job_id : str
            The job_id which all the retrieved calls from storage must have.

        Raises
        ------
        StorageError :
            If the job was not found for the given job_id
        """
