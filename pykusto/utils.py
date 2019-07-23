import json
import logging
from datetime import datetime, timedelta
from typing import Union, Mapping, NewType, Type, Dict, Callable, Any, Tuple, List

logger = logging.getLogger("pykusto")

KustoTypes = Union[str, int, bool, datetime, Mapping, List, Tuple, float, timedelta]
# TODO: Unhandled date types: guid, decimal

KQL = NewType('KQL', str)


def datetime_to_kql(dt: datetime) -> KQL:
    return KQL(dt.strftime('datetime(%Y-%m-%d %H:%M:%S.%f)'))


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


def dynamic_to_kql(d: Union[Mapping, List, Tuple]) -> KQL:
    return KQL(json.dumps(d))


def bool_to_kql(b: bool) -> KQL:
    return KQL('true') if b else KQL('false')


def str_to_kql(s: str) -> KQL:
    return KQL('"{}"'.format(s))


KQL_CONVERTER_BY_TYPE: Dict[Type, Callable[[Any], KQL]] = {
    datetime: datetime_to_kql,
    timedelta: timedelta_to_kql,
    Mapping: dynamic_to_kql,
    List: dynamic_to_kql,
    Tuple: dynamic_to_kql,
    bool: bool_to_kql,
    str: str_to_kql,
    int: KQL,
    float: KQL,
}


def to_kql(obj: KustoTypes) -> KQL:
    for kusto_type, converter in KQL_CONVERTER_BY_TYPE.items():
        if isinstance(obj, kusto_type):
            return converter(obj)
    raise ValueError("No KQL converter found for object {} of type {}".format(obj, type(obj)))
