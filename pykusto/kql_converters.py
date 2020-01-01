import json
from datetime import datetime, timedelta
from numbers import Number
from typing import NewType, Mapping, Union, List, Tuple

from pykusto.type_utils import kql_converter

KQL = NewType('KQL', str)


@kql_converter(datetime)
def datetime_to_kql(dt: datetime) -> KQL:
    return KQL(dt.strftime('datetime(%Y-%m-%d %H:%M:%S.%f)'))


@kql_converter(timedelta)
def timedelta_to_kql(td: timedelta) -> KQL:
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return KQL('time({days}.{hours}:{minutes}:{seconds}.{microseconds})'.format(
        days=td.days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=td.microseconds,
    ))


@kql_converter(Mapping, List, Tuple)
def dynamic_to_kql(d: Union[Mapping, List, Tuple]) -> KQL:
    query = list(json.dumps(d))
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


@kql_converter(bool)
def bool_to_kql(b: bool) -> KQL:
    return KQL('true') if b else KQL('false')


@kql_converter(str)
def str_to_kql(s: str) -> KQL:
    return KQL('"{}"'.format(s))


@kql_converter(Number)
def number_to_kql(n: Number) -> KQL:
    return KQL(str(n))


@kql_converter(type(None))
def none_to_kql(n: type(None)) -> KQL:
    return KQL("")
