"""Small process-local cache for the two daily reporting snapshots.

Cloud Run instances/workers do not share this cache. Expired entries never
serve as a fallback after a source error. Authentication belongs to the route.
"""
from concurrent.futures import Future
from copy import deepcopy
from threading import Lock
from time import monotonic


class ReportCache:
    def __init__(self, ttl_seconds=30, clock=monotonic):
        self.ttl = max(0, min(float(ttl_seconds), 300))
        self.clock = clock
        self.lock = Lock()
        self.entries = {}
        self.pending = {}

    def get(self, key, load):
        if key not in ('clients', 'nav'):
            raise ValueError('Unsupported report cache key')
        if self.ttl == 0:
            return load()
        with self.lock:
            entry = self.entries.get(key)
            if entry and self.clock() < entry[0]:
                return deepcopy(entry[1])
            future = self.pending.get(key)
            owner = future is None
            if owner:
                future = self.pending[key] = Future()
        if not owner:
            return deepcopy(future.result())
        try:
            value = deepcopy(load())
            with self.lock:
                self.entries[key] = (self.clock() + self.ttl, value)
                future.set_result(value)
                del self.pending[key]
            return deepcopy(value)
        except BaseException as error:
            with self.lock:
                future.set_exception(error)
                del self.pending[key]
                self.entries.pop(key, None)
            raise
