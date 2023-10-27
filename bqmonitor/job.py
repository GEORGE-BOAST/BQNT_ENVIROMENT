import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

_INITIAL_STAGE_NAME = "initial"


@dataclass
class Job:
    """
    Description
    -----------
    An object storing the job meta data.

    Attributes
    -----------
    job_id: str -
        unique job identifier.

    call_ids: List[str] -
        ids of calls made from this job.

    Methods
    -------
    to_dict -
        A method to convert this job to JSON dict.

    from_dict -
        A method to a JSON dict to job.

    """

    def __init__(
        self, job_id=None, call_ids: List[str] = None, stages: List[str] = None
    ) -> None:
        self.job_id: str = job_id or uuid.uuid4().hex
        # we need an ordered set to quickly check if a call id is in our Job,
        # but also know the order of calls coming into the monitor. Because
        # python has no orderedset, we remove our duplicated and maintain this here
        call_ids = call_ids or []
        self.call_ids: List[str] = list(dict.fromkeys(call_ids))
        self._call_ids_as_set: Set[str] = set(call_ids)
        self.stages: List[str] = stages or [_INITIAL_STAGE_NAME]

    def add(self, call_id: str) -> None:
        """
        Description
        -----------
        Adds call_id to the list of calls if call_id is not already in the list,
        otherwise does nothing

        Parameters
        ----------
        call_id: str -
            the id of the call to add to the job's list of calls
        """
        if call_id not in self._call_ids_as_set:
            self.call_ids.append(call_id)
            self._call_ids_as_set.add(call_id)

    def __repr__(self) -> str:
        return f"Job<job_id={self.job_id}, call_ids={self.call_ids}>"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, self.__class__):
            raise TypeError
        return (
            self.job_id == __o.job_id
            and self.call_ids == __o.call_ids
            and self._call_ids_as_set == __o._call_ids_as_set
            and self.stages == __o.stages
        )

    def __contains__(self, __o: object) -> bool:
        return __o in self._call_ids_as_set

    def __hash__(self):
        return hash(self.job_id)

    def to_dict(self) -> Dict[str, Any]:
        """
        Description
        -----------
        Convert Job to dict.

        Returns
        -------
        Dict[str, Any] -
            converted job.
        """
        return {
            "job_id": self.job_id,
            "call_ids": self.call_ids,
            "stages": self.stages,
        }

    @staticmethod
    def from_dict(json_dct: Dict[str, Any]) -> "Job":
        """
        Description
        -----------
        Convert JSON dict to Job.

        Parameters
        ----------
        json_dct : Dict[str, Any] -
            JSON dict to convert.

        Returns
        -------
        Job : -
            converted job.
        """
        return Job(
            job_id=json_dct["job_id"],
            call_ids=json_dct.get("call_ids", None),
            stages=json_dct.get("stages", [_INITIAL_STAGE_NAME]),
        )
