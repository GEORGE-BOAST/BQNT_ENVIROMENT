import logging
import os
from typing import ByteString, Callable, Dict, List, Union

from bqmonitor.call import Call
from bqmonitor.job import Job
from bqmonitor.storage.abstract_store import AbstractStore, StorageError
from bqmonitor.util import mkdir_if_none, read_json, write_json

_logger = logging.getLogger(__name__)

_PROCESS_FILE_NAME = "job"
_FILE_STORE_METADATA_FILE_NAME = "file-store-metadata"


class FileStore(AbstractStore):
    """
    Description
    -----------
    A storage client which stores files in the local file store of the computer.

    Also See
    --------
    >>> bqmonitor.storage.abstract_store
    """

    def __init__(self, base_dir: str = "bqmonitor-storage") -> None:
        """
        Parameters
        ----------
        base_dir : str, default = "bqmonitor-storage"
            The path and name of the base directory for storing bqmonitor data
        """
        self.__base_dir = base_dir
        self._call_counter = 0
        self.__call_id_to_filename: Dict[str, str] = {}
        self.__metadata_path = self.__create_file_path(
            _FILE_STORE_METADATA_FILE_NAME
        )

    def store_job(self, job: Job) -> None:
        """
        Description
        -----------
        Store job in the job job_id directory as JSON file.

        Parameters
        -----------
        job: Job
            Job to store.

        Returns
        --------
        None

        Example
        -------
        >>> file_store = FileStore(base_dir="base")
        >>> job = Job(job_id="abc", call_ids=["call-1"])
        >>> file_store.store_job(job)
        >>> # Result: the job is stored as a JSON file, 'base/abc/job'.
        """

        _logger.info("storing job: %s", str(job))

        try:
            job_path = self.__create_job_path(job.job_id)
            write_json(job.to_dict(), file=job_path)
        except Exception as e:
            msg = f"error storing job {job.job_id}"
            _logger.error(f"{msg} - %s", e)
            raise StorageError(msg) from e

    def get_job(self, job_id: str) -> Job:
        """
        Description
        -----------
        Return the job with the id 'job_id'

        Parameters
        -----------
        job_id : str -
            the id of the job to return.

        Returns
        --------
        Job
        """

        _logger.info("getting job: %s", str(job_id))

        try:
            job_path = self.__create_job_path(job_id)
            json_dict = read_json(job_path)
            return Job.from_dict(json_dict)
        except FileNotFoundError:
            raise StorageError(
                f"Could not find the job '{job_id}' in storage",
            )
        except Exception as e:
            msg = f"error getting job with id: {str(job_id)}"
            _logger.error(f"{msg} - %s", e)
            raise StorageError(msg) from e

    def __create_job_path(self, job_id: str) -> Union[str, bytes]:
        return self.__create_file_path(job_id, _PROCESS_FILE_NAME)

    def __create_file_path(self, *paths: str) -> Union[str, bytes]:
        return os.path.join(self.__base_dir, *list(paths))

    def store_call(self, call: Call) -> None:
        """
        Description
        -----------
        Store call in its parent job directory.

        Parameters
        -----------
        call: Call
            Call to store.

        Returns
        --------
        None
        """
        if not call.job_id:
            raise ValueError("Expected call to have job but job_id is None")
        if not call.stage:
            raise ValueError("Expected call to have stage but stage is None")
        if not call.replay_key:
            raise ValueError(
                "Expected call to have a repaly_key but repaly_key is None"
            )
        _logger.info(f"storing job: {call.job_id} call: {call.call_id}")
        try:
            # write the call name to storage metadata if it's not already there
            if call.call_id not in self.__call_id_to_filename:
                self.__call_id_to_filename[
                    call.call_id
                ] = f"call_{self._call_counter}"
                # update our metadata so we know the file name for this call id to
                # get it from storage in the future
                write_json(
                    self.__call_id_to_filename, file=self.__metadata_path
                )
                self._call_counter += 1
            # store the call in its call file
            call_file_path = self.__create_file_path(
                call.job_id, self.__call_id_to_filename[call.call_id]
            )
            write_json(call.to_dict(), file=call_file_path)
        except Exception as e:
            msg = f"error storing call: {call.call_id} for job: {call.job_id}"
            _logger.error(f"{msg} - %s", e)
            raise StorageError(msg) from e

    def get_calls(self, job: Job) -> List[Call]:
        """
        Description
        -----------
        Get all calls for the given job.

        Parameters:
        -----------
        job: Job
            Job to get calls for.

        Returns:
        --------
        List[Call]
        """
        _logger.info("getting call for job: %s", str(job))
        try:
            call_metadata = read_json(self.__metadata_path)
        except FileNotFoundError:
            write_json(self.__call_id_to_filename, self.__metadata_path)
            call_metadata = {}
        self.__call_id_to_filename = {
            **self.__call_id_to_filename,
            **call_metadata,
        }

        def get_call(call_id: str) -> Call:
            try:
                try:
                    call_file_name = self.__call_id_to_filename[call_id]
                except KeyError:
                    raise StorageError(
                        "Could not find file name for call %s in stored metadata",
                        call_id,
                    )
                call_path = self.__create_file_path(job.job_id, call_file_name)
                json_dct = read_json(call_path)
                return Call.from_dict(json_dct)
            except Exception as e:
                msg = f"error getting call: {call_id} from storage"
                _logger.error(f"{msg} - %s", e)
                raise StorageError(msg) from e

        return [get_call(call_id) for call_id in job.call_ids]
