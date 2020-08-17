from enum import Enum


class Order(Enum):
    ASC = "asc"
    DESC = "desc"


class Nulls(Enum):
    FIRST = "first"
    LAST = "last"


class JoinKind(Enum):
    INNERUNIQUE = "innerunique"
    INNER = "inner"
    LEFTOUTER = "leftouter"
    RIGHTOUTER = "rightouter"
    FULLOUTER = "fullouter"
    LEFTANTI = "leftanti"
    ANTI = "anti"
    LEFTANTISEMI = "leftantisemi"
    RIGHTANTI = "rightanti"
    RIGHTANTISEMI = "rightantisemi"
    LEFTSEMI = "leftsemi"
    RIGHTSEMI = "rightsemi"


class Distribution(Enum):
    SINGLE = 'single'
    PER_NODE = 'per_node'
    PER_SHARD = 'per_shard'


class BagExpansion(Enum):
    BAG = "bag"
    ARRAY = "array"


class Kind(Enum):
    NORMAL = 'normal'
    REGEX = 'regex'
