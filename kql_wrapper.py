from utils import KQL


class KQLWrapper:
    kql: str

    def __init__(self, kql: KQL) -> None:
        self.kql = kql
