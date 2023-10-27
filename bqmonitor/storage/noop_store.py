import logging
from typing import List

from bqmonitor.call import Call
from bqmonitor.job import Job
from bqmonitor.storage.abstract_store import AbstractStore

_logger = logging.getLogger(__name__)


class NoopStore(AbstractStore):
    """
    Description
    -----------
    A dummy store which does nothing on a publish call and returns falsey values
    when called. This is the store is set when we disable monitoring on bqmonitor
    """

    @staticmethod
    def warn_logging_disabled() -> None:
        _logger.warning(
            (
                "Calling monitor methods with bqmonitor disabled.",
                "To enable bqmonitor call bqmonitor.enable()",
            )
        )

    def store_job(self, job: Job) -> None:
        self.warn_logging_disabled()
        return None

    def get_job(self, job_id: str) -> Job:
        self.warn_logging_disabled()
        raise ValueError(f"No such job found for job_id: {job_id}")

    def store_call(self, call: Call) -> None:
        self.warn_logging_disabled()
        return None

    def get_calls(self, job: Job) -> List[Call]:
        self.warn_logging_disabled()
        return []
