from concurrent.futures import Future, ThreadPoolExecutor, wait
from itertools import chain
from threading import Lock
from typing import Union, Dict, Any, Iterable, Callable

# Using a thread pool even though we only need one thread, because that's the only way to make use of "futures".
# Also, this makes it easy to use more than one thread, if the need ever arises.
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

    def __init__(self, items: Union[None, Dict[str, Any]], retrieve_by_default: bool) -> None:
        self._lock = Lock()
        self._future = None
        self._retrieve_by_default = retrieve_by_default
        self._items = items
        if items is None and retrieve_by_default:
            self.refresh()

    def _new_item(self, name: str) -> Any:
        raise NotImplementedError()

    def __getattr__(self, name: str) -> Any:
        """
        Convenience function for retrieving an item using dot notation.
        Often dot notation is used for other purposes, and sometimes that happens implicitly. For example Jupyter notebooks automatically run dot-notation code in the background
        on objects. For this reason, to avoid undesired erroneous queries sent to Kusto, an item is retrieved only if one already exists, and a new item is not generated otherwise
        (in contrast to bracket notation).

        :param name: Name of item retrieve
        :return: The retrieved item
        :raises AttributeError: If there is no such item
        """
        return self._get_item(name, lambda: _raise(AttributeError(f"{self} has no attribute '{name}'")))

    def __getitem__(self, name: str) -> Any:
        """
        Convenience function for retrieving an item using bracket notation.
        Since bracket notation is only used explicitly, a new item is generated if needed (in contrast to dot notation).

        :param name: Name of item retrieve
        :return: The retrieved item
        """
        return self._get_item(name, lambda: self._new_item(name))

    def _get_item(self, name: str, fallback: Callable) -> Any:
        if self._items is not None:
            resolved_item = self._items.get(name)
            if resolved_item is not None:
                return resolved_item
        return fallback()

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(super().__dir__(), tuple() if self._items is None else self._items.keys()))

    def refresh(self):
        """
        Retrieves all items in a separate thread, making them available after the tread finishes executing. The 'wait_for_items' method can be used to wait for that to happen.
        The specific retrieval logic is defined in concrete subclasses.
        """
        self._future = POOL.submit(self._get_items)
        self._future.add_done_callback(self._set_items)

    def wait_for_items(self):
        """
        If item retrieval is currently in progress, wait until it is done and return, otherwise return immediately.
        If several retrievals are in progress, wait for the most recent one.
        """
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


def _raise(e: BaseException):
    raise e
