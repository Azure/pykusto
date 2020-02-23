from concurrent.futures import Future, ThreadPoolExecutor, wait
from itertools import chain
from threading import Lock
from typing import Union, Dict, Any, Iterable

POOL = ThreadPoolExecutor(max_workers=1)


class Retriever:
    _items: Union[None, Dict[str, Any]]
    _future: Union[None, Future]
    _lock: Lock

    def __init__(self) -> None:
        self._lock = Lock()
        self._future = None
        if self._items is None:
            self.refresh()

    def new_item(self, name: str) -> Any:
        raise NotImplementedError()

    def get_item(self, name: str) -> Any:
        if self._items is None:
            return self.new_item(name)
        resolved_item = self._items.get(name)
        if resolved_item is None:
            return self.new_item(name)
        return resolved_item

    def __getattr__(self, name: str) -> Any:
        return self.get_item(name)

    def __getitem__(self, name: str) -> Any:
        return self.get_item(name)

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
