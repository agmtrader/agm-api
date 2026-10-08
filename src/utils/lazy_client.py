"""Delay expensive connector construction until its first operation."""
from threading import Lock


class LazyClient:
    def __init__(self, factory):
        self._factory = factory
        self._client = None
        self._lock = Lock()

    def __getattr__(self, name):
        with self._lock:
            if self._client is None:
                self._client = self._factory()
            client = self._client
        return getattr(client, name)
