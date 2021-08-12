import os
import sys

from setuptools import setup

assert sys.version_info[0] == 3
__version__ = None
with open(os.path.join('.', 'pykusto', '__init__.py')) as f:
    for line in f:
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            __version__ = line.split(delim)[1]
assert __version__ is not None, 'Unable to determine version'

core_requires = [
    'redo==2.0.4',
]

# Set PYSPARK=1 as an environment variable to avoid the following dependencies
non_pyspark_requires = [
    # Not required in PySpark, because authentication is handled differently there.
    # Release notes: https://github.com/Azure/azure-kusto-python/releases
    'azure-kusto-data==2.1.1',  # Earlier versions not supported because of: https://github.com/Azure/azure-kusto-python/issues/312
]

# Not required in PySpark because it is already installed there.
# pandas release notes: https://pandas.pydata.org/docs/whatsnew/index.html
# Tests use DataFrame constructor options introduced in 0.25.0
if sys.version_info[1] <= 6:
    # pandas support for Python 3.6 was dropped starting from version 1.2.0
    non_pyspark_requires.append('pandas>=0.25.0,<1.2.0')
    # In numpy the support was dropped in 1.20.0, and also the transitive dependency in pandas is not correctly restricted
    non_pyspark_requires.append('numpy<1.20.0')
else:
    non_pyspark_requires.append('pandas>=0.25.0,<=1.2.4')

setup(
    name='pykusto',
    version=__version__,
    packages=['pykusto'],
    url='https://github.com/Azure/pykusto',
    license='MIT License',
    author='Microsoft Corporation',
    author_email='yomost@microsoft.com',
    description='Advanced python SDK for Azure Data Explorer',
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    keywords="kusto azure-data-explorer client library query",
    install_requires=core_requires + ([] if os.getenv('PYSPARK', False) else non_pyspark_requires),
    extras_require={
        'test': [
            'pytest',
            'pytest-cov',
            'flake8',
            'typeguard',
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "License :: OSI Approved :: MIT License",
        # Make sure this list matches the tested versions in runtests.yml
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
    ],
)
