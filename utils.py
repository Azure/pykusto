import json
from datetime import datetime, timedelta
from typing import Union, Sequence, Mapping

KustoTypes = Union[str, int, bool, datetime, Mapping, Sequence, float, timedelta]
# TODO: Unhandled date types: guid, decimal


def datetime_to_kql(dt: datetime) -> str:
    return dt.strftime('datetime(%Y-%m-%d %H:%M:%S.%f)')


def timedelta_to_kql(td: timedelta) -> str:
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return 'time({days}.{hours}:{minutes}:{seconds}.{microseconds})'.format(
        days=td.days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=td.microseconds,
    )


def dynamic_to_kql(d: Union[Mapping, Sequence]) -> str:
    return json.dumps(d)


def bool_to_kql(b: bool) -> str:
    return 'true' if b else 'false'


KQL_CONVERTER_BY_TYPE = {
    datetime: datetime_to_kql,
    timedelta: timedelta_to_kql,
    Mapping: dynamic_to_kql,
    Sequence: dynamic_to_kql,
    bool: bool_to_kql,
}


def to_kql(o: KustoTypes) -> str:
    for kusto_type, converter in KQL_CONVERTER_BY_TYPE.items():
        if isinstance(o, kusto_type):
            return converter(o)
