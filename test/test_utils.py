from pykusto.expressions import to_kql
from pykusto.kql_converters import KQL
from pykusto.type_utils import TypeRegistrar, KustoType
from test.test_base import TestBase


class TestUtils(TestBase):
    def test_dynamic_to_kql(self):
        test_dict = {
            "name": "Alan",
            "age": 21,
            "address": ("NY", 36),
            "pets": ["Libby", "Panda", "]", "["]
        }
        self.assertEqual(
            '{"name": "Alan", "age": 21, "address": ("NY", 36), '
            '"pets": ("Libby", "Panda", "]", "[")}',
            to_kql(test_dict)
        )

    def test_type_registrar_for_type(self):
        test_annotation = TypeRegistrar("Test annotation")

        @test_annotation(KustoType.STRING)
        def str_annotated(s: str) -> str:
            return "response to " + s

        self.assertEqual(
            "response to test for_type",
            test_annotation.for_type(str)("test for_type")
        )

    def test_type_registrar_for_obj(self):
        test_annotation = TypeRegistrar("Test annotation")

        @test_annotation(KustoType.STRING)
        def str_annotated(s: str) -> str:
            return "response to " + s

        self.assertEqual(
            "response to test for_obj",
            test_annotation.for_obj("test for_obj")
        )

    def test_type_registrar_for_type_not_found(self):
        test_annotation = TypeRegistrar("Test annotation")

        @test_annotation(KustoType.STRING)
        def str_annotated(s: str) -> str:
            return "response to " + s

        self.assertRaises(
            ValueError("Test annotation: no registered callable for type bool"),
            lambda: test_annotation.for_type(bool)("test for_type")
        )

    def test_type_registrar_for_obj_not_found(self):
        test_annotation = TypeRegistrar("Test annotation")

        @test_annotation(KustoType.STRING)
        def str_annotated(s: str) -> str:
            return "response to " + s

        self.assertRaises(
            ValueError("Test annotation: no registered callable for object True of type bool"),
            lambda: test_annotation.for_obj(True)
        )

    def test_type_registrar_collision(self):
        test_annotation = TypeRegistrar("Test annotation")

        @test_annotation(KustoType.STRING)
        def str_annotated_1(s: str) -> KQL:
            return KQL("response to " + s)

        def str_annotated_2(s: str) -> KQL:
            return KQL("response to " + s)

        self.assertRaises(
            TypeError("Test annotation: type already registered: string"),
            lambda: test_annotation(KustoType.STRING)(str_annotated_2)
        )
