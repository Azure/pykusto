from pykusto.expressions import to_kql
from test.test_base import TestBase


class TestUtils(TestBase):
    def test_dynamic_to_kql(self):
        dict ={
            "name": "Alan",
            "age": 21,
            "address": ("NY", 36),
            "pets": ["Libby", "Panda", "]", "["]
        }
        self.assertEqual(
            "{\"name\": \"Alan\", \"age\": 21, \"address\": (\"NY\", 36), \"pets\": (\"Libby\", \"Panda\", \"]\", \"[\")}",
            to_kql(dict)
        )
