from abc import ABCMeta, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor, wait
from itertools import chain
from threading import Lock
from typing import Union, Dict, Any, Iterable, Callable, Generator

# Using a thread pool even though we only need one thread, because that's the only way to make use of "futures".
# Also, this makes it easy to use more than one thread, if the need ever arises.
POOL = ThreadPoolExecutor(max_workers=1)


class ItemFetcher(metaclass=ABCMeta):
    """
    Abstract class that caches a collection of items, fetching them in certain scenarios.
    """
    _fetch_by_default: bool
    __items: Union[None, Dict[str, Any]]
    __future: Union[None, Future]
    __item_write_lock: Lock
    __item_fetch_lock: Lock

    def __init__(self, items: Union[None, Dict[str, Any]], fetch_by_default: bool) -> None:
        """
        :param items: Initial items. If not None, items will not be fetched until the "refresh" method is explicitly called.
        :param fetch_by_default: When true, items will be fetched in the constructor, but only if they were not supplied as a parameter. Subclasses are encouraged to pass
                                    on the value of "fetch_by_default" to child ItemFetchers.
        """
        self._fetch_by_default = fetch_by_default
        self.__items = items
        self.__future = None
        self.__item_write_lock = Lock()
        self.__item_fetch_lock = Lock()

    def _items_fetched(self) -> bool:
        return self.__items is not None

    def _refresh_if_needed(self) -> None:
        if self.__items is None and self._fetch_by_default:
            self.refresh()

    def _get_item_names(self) -> Generator[str, None, None]:
        if self.__items is None:
            return
        # No race condition because once fetched self.__items will never go back to being None
        yield from self.__items.keys()

    def _get_items(self) -> Generator[Any, None, None]:
        if self.__items is None:
            return
        yield from self.__items.values()

    @abstractmethod
    def _new_item(self, name: str) -> Any:
        raise NotImplementedError()  # pragma: no cover

    def __getattr__(self, name: str) -> Any:
        """
        Convenience function for obtaining an item using dot notation.
        Often dot notation is used for other purposes, and sometimes that happens implicitly. For example Jupyter notebooks automatically run dot-notation code in the background
        on objects. For this reason, to avoid undesired erroneous queries sent to Kusto, an item is returned only if one already exists, and a new item is not generated otherwise
        (in contrast to bracket notation).

        :param name: Name of item to return
        :return: The item with the given name
        :raises AttributeError: If there is no such item
        """
        return self._get_item(name, lambda: _raise(AttributeError(f"{self} has no attribute '{name}'")))

    def __getitem__(self, name: str) -> Any:
        """
        Convenience function for obtaining an item using bracket notation.
        Since bracket notation is only used explicitly, a new item is generated if needed (in contrast to dot notation).

        :param name: Name of item to return
        :return: The item with the given name
        """
        return self._get_item(name, lambda: self.__generate_and_save_new_item(name))

    def __generate_and_save_new_item(self, name: str) -> Any:
        item = self._new_item(name)
        with self.__item_write_lock:
            if self.__items is None:
                self.__items = {}
            self.__items[name] = item
        return item

    def _get_item(self, name: str, fallback: Callable[[], Any]) -> Any:
        if self.__items is not None:
            resolved_item = self.__items.get(name)
            if resolved_item is not None:
                return resolved_item
        return fallback()

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(super().__dir__(), tuple() if self.__items is None else filter(lambda name: '.' not in name, self.__items.keys())))

    def refresh(self) -> None:
        """
        Fetches all items in a separate thread, making them available after the tread finishes executing. The 'wait_for_items' method can be used to wait for that to happen.
        The specific logic for fetching is defined in concrete subclasses.
        """
        self.__future = POOL.submit(self.__fetch_items)
        self.__future.add_done_callback(self._set_items)

    def wait_for_items(self) -> None:
        """
        If item fetching is currently in progress, wait until it is done and return, otherwise return immediately.
        If several fetching threads are in progress, wait for the most recent one.
        """
        if self.__future is not None:
            wait((self.__future,))

    def blocking_refresh(self) -> None:
        self.refresh()
        self.wait_for_items()

    def _set_items(self, future: Future):
        with self.__item_write_lock:
            self.__items = future.result()

    @abstractmethod
    def _internal_get_items(self) -> Dict[str, Any]:
        raise NotImplementedError()  # pragma: no cover

    def __fetch_items(self) -> Dict[str, Any]:
        with self.__item_fetch_lock:
            return self._internal_get_items()


def _raise(e: BaseException):
    raise e
