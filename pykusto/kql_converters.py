import json
from datetime import datetime, timedelta
from itertools import chain
from numbers import Number
from typing import NewType, Union, Mapping, List, Tuple

from pykusto.type_utils import kql_converter, KustoType, NUMBER_TYPES

KQL = NewType('KQL', str)


@kql_converter(KustoType.DATETIME)
def datetime_to_kql(dt: datetime) -> KQL:
    return KQL(dt.strftime('datetime(%Y-%m-%d %H:%M:%S.%f)'))


@kql_converter(KustoType.TIMESPAN)
def timedelta_to_kql(td: timedelta) -> KQL:
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return KQL(f'time({td.days}.{hours}:{minutes}:{seconds}.{td.microseconds})')


@kql_converter(KustoType.ARRAY, KustoType.MAPPING)
def dynamic_to_kql(d: Union[Mapping, List, Tuple]) -> KQL:
    try:
        query = list(json.dumps(d))
    except TypeError:
        # Using exceptions as part of normal flow is not best practice, however in this case we have a good reason.
        # The given object might contain a non-primitive object somewhere inside it, and the only way to find it is to go through the entire hierarchy, which is exactly
        # what's being done in the process of json conversion.
        # Also keep in mind that exception handling in Python has no performance overhead (unlike e.g. Java).
        return build_dynamic(d)

    # Convert square brackets to round brackets (Issue #11)
    counter = 0
    prev = ""
    for i, c in enumerate(query):
        if counter == 0:
            if c == "[":
                query[i] = "("
            elif c == "]":
                query[i] = ")"
            elif c in ['"', '\''] and prev != "\\":
                counter += 1
        elif counter > 0:
            if c in ['"', '\''] and prev != "\\":
                counter -= 1
        prev = query[i]
    assert counter == 0
    return KQL("".join(query))


def build_dynamic(d: Union[Mapping, List, Tuple]) -> KQL:
    from pykusto.expressions import BaseExpression
    if isinstance(d, BaseExpression):
        return d.kql
    if isinstance(d, Mapping):
        return KQL(f"pack({', '.join(map(build_dynamic, chain(*d.items())))})")
    if isinstance(d, (List, Tuple)):
        return KQL(f"pack_array({', '.join(map(build_dynamic, d))})")
    from pykusto.expressions import to_kql
    return to_kql(d)


@kql_converter(KustoType.BOOL)
def bool_to_kql(b: bool) -> KQL:
    return KQL('true') if b else KQL('false')


@kql_converter(KustoType.STRING)
def str_to_kql(s: str) -> KQL:
    return KQL(f'"{s}"')


@kql_converter(*NUMBER_TYPES)
def number_to_kql(n: Number) -> KQL:
    return KQL(str(n))


kql_converter.assert_all_types_covered()
