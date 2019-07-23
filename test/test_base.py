import logging
import sys
from unittest import TestCase


class TestBase(TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG,
            format='%(asctime)s %(levelname)s - %(message)s'
        )
