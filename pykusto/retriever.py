from concurrent.futures import Future, ThreadPoolExecutor, wait
from itertools import chain
from threading import Lock
from typing import Union, Dict, Any, Iterable

POOL = ThreadPoolExecutor(max_workers=1)


class Retriever:
    _retrieve_by_default: bool
    _items: Union[None, Dict[str, Any]]
    _future: Union[None, Future]
    _lock: Lock

    def __new__(cls, *args, **kwargs):
        if cls is 'Retriever':
            raise TypeError("Retriever is abstract")
        return object.__new__(cls)

    def __init__(self, retrieve_by_default: bool = True) -> None:
        self._lock = Lock()
        self._future = None
        self._retrieve_by_default = retrieve_by_default
        if retrieve_by_default and self._items is None:
            self.refresh()

    def _new_item(self, name: str) -> Any:
        raise NotImplementedError()

    def _get_item(self, name: str) -> Any:
        if self._items is None:
            return self._new_item(name)
        resolved_item = self._items.get(name)
        if resolved_item is None:
            return self._new_item(name)
        return resolved_item

    def __getattr__(self, name: str) -> Any:
        if self._items is None:
            raise AttributeError()
        resolved_item = self._items.get(name)
        if resolved_item is None:
            raise AttributeError()
        return resolved_item

    def __getitem__(self, name: str) -> Any:
        return self._get_item(name)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(super().__dir__(), tuple() if self._items is None else self._items.keys()))

    def refresh(self):
        self._future = POOL.submit(self._get_items)
        self._future.add_done_callback(self._set_items)

    # For use mainly in tests
    def wait_for_items(self):
        if self._future is not None:
            wait((self._future,))

    def _set_items(self, future: Future):
        with self._lock:
            self._items = future.result()

    def _internal_get_items(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def _get_items(self) -> Dict[str, Any]:
        with self._lock:
            return self._internal_get_items()
