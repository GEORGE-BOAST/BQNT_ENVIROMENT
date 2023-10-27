from bqmonitor.permissions import is_enabled_for_bqmonitor_devtools
from bqmonitor.storage.abstract_store import AbstractStore, StorageError
from bqmonitor.storage.file_store import FileStore
from bqmonitor.storage.memory_store import MemoryStore
from bqmonitor.storage.noop_store import NoopStore

__all__ = [
    "AbstractStore",
    "NoopStore",
    "MemoryStore",
    "FileStore",
    "StorageError",
]

if is_enabled_for_bqmonitor_devtools():
    from bqmonitor.storage.bcs_store import BCSStore

    __all__.append("BCSStore")
