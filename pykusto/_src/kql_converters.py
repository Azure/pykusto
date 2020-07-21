import json
from datetime import datetime, timedelta
from itertools import chain
from numbers import Number
from typing import NewType, Union, Mapping, List, Tuple

from .type_utils import _kql_converter, _KustoType, _NUMBER_TYPES

KQL = NewType('KQL', str)


@_kql_converter(_KustoType.DATETIME)
def _datetime_to_kql(dt: datetime) -> KQL:
    return KQL(dt.strftime('datetime(%Y-%m-%d %H:%M:%S.%f)'))


@_kql_converter(_KustoType.TIMESPAN)
def _timedelta_to_kql(td: timedelta) -> KQL:
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return KQL(f'time({td.days}.{hours}:{minutes}:{seconds}.{td.microseconds})')


@_kql_converter(_KustoType.ARRAY, _KustoType.MAPPING)
def _dynamic_to_kql(d: Union[Mapping, List, Tuple]) -> KQL:
    try:
        return KQL(f"dynamic({json.dumps(d)})")
    except TypeError:
        # Using exceptions as part of normal flow is not best practice, however in this case we have a good reason.
        # The given object might contain a non-primitive object somewhere inside it, and the only way to find it is to go through the entire hierarchy, which is exactly
        # what's being done in the process of json conversion.
        # Also keep in mind that exception handling in Python has no performance overhead (unlike e.g. Java).
        return _build_dynamic(d)


def _build_dynamic(d: Union[Mapping, List, Tuple]) -> KQL:
    if isinstance(d, Mapping):
        return KQL(f"pack({', '.join(map(_build_dynamic, chain(*d.items())))})")
    if isinstance(d, (List, Tuple)):
        return KQL(f"pack_array({', '.join(map(_build_dynamic, d))})")
    from .expressions import _to_kql
    return _to_kql(d)


@_kql_converter(_KustoType.BOOL)
def _bool_to_kql(b: bool) -> KQL:
    return KQL('true') if b else KQL('false')


@_kql_converter(_KustoType.STRING)
def _str_to_kql(s: str) -> KQL:
    return KQL(f'"{s}"')


@_kql_converter(*_NUMBER_TYPES)
def _number_to_kql(n: Number) -> KQL:
    return KQL(str(n))


_kql_converter.assert_all_types_covered()
