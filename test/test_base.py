import logging
import sys
from typing import Callable
from unittest import TestCase

from pykusto.utils import logger


class TestBase(TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG,
            format='%(asctime)s %(levelname)5s %(message)s'
        )

    def setUp(self):
        logger.info("Running test: " + self._testMethodName)

    def assertRaises(self, expected_exception: BaseException, test_callable: Callable, *args, **kwargs):
        expected_exception_type = type(expected_exception)
        expected_exception_message = str(expected_exception)
        with super().assertRaises(expected_exception_type) as cm:
            test_callable(*args, **kwargs)
        self.assertEqual(
            expected_exception_message,
            str(cm.exception)
        )
