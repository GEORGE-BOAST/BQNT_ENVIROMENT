import json
import logging
import os
from typing import List

from bqmonitor.call import Call
from bqmonitor.job import Job
from bqmonitor.permissions import is_enabled_for_bqmonitor_devtools
from bqmonitor.storage.abstract_store import AbstractStore, StorageError

_HAS_BOTO3 = True
try:
    import boto3
    from botocore.client import ClientError
except ImportError:
    _HAS_BOTO3 = False

_logger = logging.getLogger(__name__)

_BUCKET_NAME = "bqmonitor"
_BCS_ACCESS_KEY = os.environ.get("BCS_ACCESS_KEY")
_BCS_SECRET_KEY = os.environ.get("BCS_SECRET_KEY")


class BCSStore(AbstractStore):
    def __init__(self) -> None:
        if not is_enabled_for_bqmonitor_devtools():
            raise StorageError(
                "BCS Store is only for Bloomberg employees and requires BREG enablement"
            )
        if not _HAS_BOTO3:
            raise StorageError("BCS Store requires boto3 to be installed.")
        if not _BCS_ACCESS_KEY or not _BCS_SECRET_KEY:
            raise StorageError(
                "Expected BCS_ACCESS_KEY and BCS_SECRET_KEY to be passed as environment variables"
            )
        self._boto3_resource = boto3.resource(
            "s3",
            aws_access_key_id=_BCS_ACCESS_KEY,
            aws_secret_access_key=_BCS_SECRET_KEY,
            endpoint_url="https://s3.dev.obdc.bcs.bloomberg.com",
        )
        self.bucket = self.__create_bucket_if_none()

    def __create_bucket_if_none(self):
        bucket = self._boto3_resource.Bucket(_BUCKET_NAME)
        if not self.__bucket_exists():
            try:
                bucket.create()
            except:
                msg = f"error creating bucket {_BUCKET_NAME}"
                _logger.error(msg, exc_info=True)
                raise StorageError(msg)
        return bucket

    def __bucket_exists(self):
        try:
            # If bucket already exists, this will succeed
            self._boto3_resource.meta.client.head_bucket(Bucket=_BUCKET_NAME)
        except ClientError as error:
            _logger.error(error)
            return False
        else:
            return True

    def store_job(self, job: Job) -> None:
        """
        Store job in BCS as JSON object.

        Raises
        ------
        StorageError
            If attempt to store job in bqmonitor bucket fails

        Example
        -------
        >>> bcs_store = BCSStore()
        >>> job = Job(job_id="abc", call_ids=["call-1"])
        >>> bcs_store.store_job(job)
        >>> # Result: the job is stored as a JSON file in the bqmonitor bucket
        """

        _logger.info("storing job: ", job)

        try:
            self.bucket.put_object(
                Key=job.job_id, Body=json.dumps(job.to_dict())
            )
        except Exception as e:
            msg = f"error storing job {job.job_id}"
            _logger.error(f"{msg} - %s", e)
            raise StorageError(msg) from e

    def get_job(self, job_id: str) -> Job:
        """
        Return the job with the id 'job_id'

        Raises
        ------
        StorageError
            If failure to load job from bqmonitor bucket
        """

        _logger.info("getting job : ", job_id)

        try:
            res = self._boto3_resource.Object(_BUCKET_NAME, job_id).get()
            job_as_dict = json.loads(res["Body"].read().decode("utf-8"))
            return Job.from_dict(job_as_dict)
        except Exception as e:
            msg = f"error getting job with key {job_id}"
            _logger.error(f"{msg} - %s", e)
            raise StorageError(msg) from e

    def store_call(self, call: Call) -> None:
        """
        Store call on BCS.

        Raises
        ------
        ValueError
            If call is missing any of job_id, stage, replay_key
        StorageError
            If attempt to store call in bqmonitor bucket fails
        """
        if not call.job_id:
            raise ValueError("Expected call to have job but job_id is None")
        if not call.stage:
            raise ValueError("Expected call to have stage but stage is None")
        if not call.replay_key:
            raise ValueError(
                "Expected call to have a replay_key but replay_key is None"
            )
        _logger.info(f"storing job: {call.job_id} call: {call.call_id}")

        try:
            self.bucket.put_object(
                Key=call.job_id + "/" + call.call_id,
                Body=json.dumps(call.to_dict()),
            )
        except Exception as e:
            msg = f"error storing call: {call.call_id} for job: {call.job_id}"
            _logger.error(f"{msg} - %s", e)
            raise StorageError(msg) from e

    def get_calls(self, job: Job) -> List[Call]:
        """
        Get all calls for the given job.

        Raises
        ------
        ValueError
            If call is missing any of job_id, stage, replay_key
        StorageError
            If failure to load any one of the calls in give job from bqmonitor bucket

        """
        _logger.info("getting call for job:", job)

        def get_call(call_key):
            try:
                res = self._boto3_resource.Object(
                    _BUCKET_NAME, job.job_id + "/" + call_key
                ).get()
                call_as_dict = json.loads(res["Body"].read().decode("utf-8"))
                return Call.from_dict(call_as_dict)
            except Exception as e:
                msg = f"error getting call: {call_key} from storage"
                _logger.error(f"{msg} - %s", e)
                raise StorageError(msg) from e

        return [get_call(call_key) for call_key in job.call_ids]
