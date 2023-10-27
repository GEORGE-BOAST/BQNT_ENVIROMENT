import functools
import inspect
import os
from copy import copy
from enum import Enum, unique
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union

import bqmonitor
from bqmonitor import FileStore, Job
from bqmonitor.storage import StorageError
from bqmonitor.types import JSONSerializableType

TestCallable = Callable[
    [Tuple[Any, ...], Dict[str, Any]], JSONSerializableType
]


class TestMonitoringError(Exception):
    pass


@unique
class TestMode(Enum):
    """
    Test monitoring modes.

    - REPLAY: Replay service responses.
    - RECORD: Record service requests and responses.
    - SKIP: Skip RECORD and REPLAY.
    """

    REPLAY = "REPLAY"
    RECORD = "RECORD"
    SKIP = "SKIP"


def monitor_test(
    dataset: Optional[str] = None,
    mode: Optional[TestMode] = TestMode.RECORD,
    base_dir: Optional[str] = None,
) -> "_MonitorTestDecorator":
    """
    Monitor test by recording or replaying service requests.

    Parameters
    ----------
    base_dir : str
        Alternative directory to save service requests and responses.

    dataset : str
        Name of the set of service requests and responses in a test.

    mode : TestMode
        RECORD: record service requests and responses in AbstractStore.
        REPLAY: replay service responses from the AbstractStore.
        SKIP: skip monitoring and return requests from services.

    Returns
    -------
    _MonitorTestDecorator
        A test monitoring object with the core decorating and monitoring implementation.


    Raises
    ------
    ValueError
        if 'dataset' is None or 'mode' type is not TestMode.

    TestMonitoringError
        if service responses are missing when replaying or errors recording service data.

    Example
    ---------
    >>> # record service response in test/package/test-data/bqmonitor-storage/test-file/
    >>> @monitor_test()
    >>> def test_monitored_service_request(monitored_service_client):
    >>>     response = monitored_service_client.request(request)
    >>>

    >>> # record service response in test/package/test-data/bqmonitor-storage/bql_price_data
    >>> @monitor_test("bql_price_data")
    >>> def test_monitored_service_request(monitored_service_client):
    >>>     response = monitored_service_client.request(request)
    >>>

    >>> # replay service requests and responses from the AbstractStore 'bql_price_data' dataset
    >>> @monitor_test("bql_price_data", mode=TestMode.REPLAY)
    >>> def test_monitored_service_request(monitored_service_client):
    >>>     response = monitored_service_client.request(request)
    >>>

    >>> # skip monitoring and return service responses from service.
    >>> @monitor_test("bql_price_data", mode=TestMode.SKIP)
    >>> def test_monitored_integration_test(monitored_service_client):
    >>>     response = monitored_service_client.request(request)
    >>>

    """

    if not isinstance(mode, TestMode):
        raise ValueError(f"Invalid mode: {mode}, type should be TestMode")

    return _MonitorTestDecorator(dataset=dataset, mode=mode, base_dir=base_dir)


class _MonitorTestDecorator:
    _MONITORED_TEST = "MONITORED_TEST"

    def __init__(
        self,
        dataset: Optional[str] = None,
        mode: Optional[TestMode] = TestMode.RECORD,
        base_dir: Optional[str] = None,
        class_name: Optional[str] = None,
    ):
        self._dataset = dataset
        self._mode = mode
        self._base_dir = base_dir
        self._class_name = class_name

    def __call__(
        self, type_or_callable: Union[TestCallable, Type]
    ) -> Union[TestCallable, Type]:
        if isinstance(type_or_callable, type):
            return self.__decorate_class(type_or_callable)
        return self.__decorate_callable(type_or_callable)

    def __copy__(self) -> "_MonitorTestDecorator":
        return _MonitorTestDecorator(
            dataset=self._dataset,
            mode=self._mode,
            base_dir=self._base_dir,
            class_name=self._class_name,
        )

    def __decorate_class(self, class_: Type) -> Type:
        self._class_name = class_.__name__
        # currently we wrap anything that is a test or a setup/teardown method as
        # we assume only those methods could be making calls to backend services
        # so they must be wrapped
        # TODO: update this so we record anything user defined
        for attr in dir(class_):
            if not attr.startswith("test") and attr not in [
                "setUpClass",
                "setUp",
                "tearDown",
                "tearDownClass",
            ]:
                continue

            attr_value = getattr(class_, attr)
            if not hasattr(attr_value, "__call__"):
                continue

            decorator = copy(self)
            setattr(class_, attr, decorator(attr_value))
        return class_

    def __decorate_callable(self, func: TestCallable) -> TestCallable:
        if hasattr(func, self._MONITORED_TEST):
            return func

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> JSONSerializableType:
            mode = self.__test_mode()
            if mode == TestMode.RECORD:
                self.__record(func)
            if mode == TestMode.REPLAY:
                self.__replay(func)
            response = func(*args, **kwargs)
            bqmonitor.disable()
            return response

        setattr(wrapper, self._MONITORED_TEST, None)
        return wrapper

    def __record(self, func: TestCallable) -> None:
        base_dir = self.__storage_dir(func)
        try:
            record_service_response(base_dir=base_dir, job_id="job")
        except StorageError:
            raise TestMonitoringError(
                f"Error recording data for test {func.__name__} in {base_dir}"
            )

    def __replay(self, func: TestCallable) -> None:
        base_dir = self.__storage_dir(func)
        try:
            replay_service_response(base_dir=base_dir, job_id="job")
        except StorageError:
            raise TestMonitoringError(
                f"Error replaying data for test {func.__name__} in {base_dir}"
            )

    def __test_mode(self) -> TestMode:
        test_mode = bqmonitor_test_mode()
        if test_mode is None:
            return self._mode
        else:
            return test_mode

    def __storage_dir(self, func: TestCallable) -> str:
        def base_dir_if_none() -> Union[str, bytes]:
            if self._base_dir is None:
                return os.path.join(
                    parent_dir(func),
                    "test-data",
                    "bqmonitor-storage",
                )
            return os.path.join(self._base_dir, "bqmonitor-storage")

        def job_dir() -> Union[str, bytes]:
            def class_or_func_path() -> str:
                if self._class_name is None:
                    return func.__name__
                return os.path.join(self._class_name, func.__name__)

            return self._dataset or os.path.join(
                source_file(func), class_or_func_path()
            )

        base_dir = base_dir_if_none()
        job_dir = job_dir()
        return os.path.join(base_dir, job_dir)


def record_service_response(base_dir: str, job_id: str) -> None:
    """
    Record service response in the base directory

    Parameters
    ----------
    base_dir

    job_id
       job id to set for the underlying service client

    """
    file_store = FileStore(base_dir)
    bqmonitor.enable(file_store)
    bqmonitor.start_job(Job(job_id=job_id))


def replay_service_response(base_dir: str, job_id: str) -> None:
    """
    Replay service response from the base directory with the given job id.

    Parameters
    ----------
    base_dir
    job_id
    """
    if not os.path.exists(base_dir):
        ALLOW_UNIT_TESTS = bool(int(os.environ.get("ALLOW_UNIT_TESTS", False)))
        if ALLOW_UNIT_TESTS:
            return
        raise TestMonitoringError(
            f"""
            The dataset {base_dir} does not exist.

            TIPS:

            This is likely because you are attempting to replay code which
            did not record anything. If you think this is an error, attempt recording
            the session again. 

            To supress this error set the enviornment flag ALLOW_UNIT_TESTS=1
            """
        )
    file_store = FileStore(base_dir)
    bqmonitor.set_storage(file_store)

    try:
        bqmonitor.replay(job_id)
    except StorageError:
        raise TestMonitoringError(
            f"Error replaying service response in {base_dir}"
        )


def bqmonitor_test_mode() -> Optional[TestMode]:
    """
    Returns the bqmonitor test monitoring mode from the environment variable 'BQMONITOR_TEST_MODE'

    Returns
    -------
    TestMode or None
        - None if the 'BQMONITOR_TEST_MODE' environment variable is not set.
        - TestMode if 'record', 'replay' or 'skip' is set.

    Raises
    ______
    ValueError
        if the environment variable is not 'replay', 'record', or 'skip'.

    """
    mode = os.environ.get("BQMONITOR_TEST_MODE")
    if mode is not None:
        try:
            return TestMode(mode.upper())
        except ValueError:
            raise ValueError(
                f"Unknown mode: {mode}, should be 'replay', 'record', or 'skip'"
            )


def parent_dir(obj: object) -> Path:
    """
    Return the parent directory of the module an object was defined in.
    """
    module = inspect.getmodule(obj)
    if module.__name__ == "__main__":
        return "__main__"
    return Path(inspect.getsourcefile(module)).parent


def source_file(obj: object) -> str:
    """
    Return the filename the object was defined in.
    """
    file_path = inspect.getsourcefile(obj)
    return Path(file_path).stem
