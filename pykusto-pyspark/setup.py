from setuptools import setup

from setup import core_requires, setup_kwargs

# The 'pykusto-pyspark' package is identical to the pykusto 'package', except that it avoids dependencies which are not needed in PySpark
# (we can't use 'extras_require' for this, because it does not allow negative dependencies)

setup_kwargs['name'] = 'pykusto-pyspark'
setup_kwargs['packages'] = ['../pykusto']
setup_kwargs['description'] = setup_kwargs['description'] + '(for PySpark)'
setup_kwargs['install_requires'] = core_requires
del setup_kwargs['extras_require']


setup(**setup_kwargs)
