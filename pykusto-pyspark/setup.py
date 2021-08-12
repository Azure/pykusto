from setuptools import setup

from setup import core_requires, setup_kwargs

setup_kwargs['name'] = 'pykusto-pyspark'
setup_kwargs['description'] = setup_kwargs['description'] + '(for PySpark)'
setup_kwargs['install_requires'] = core_requires
del setup_kwargs['extras_require']


setup(**setup_kwargs)