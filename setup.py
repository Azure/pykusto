import os

from setuptools import setup, find_packages

__version__ = None
with open(os.path.join('.', 'pykusto', '__init__.py')) as f:
    for line in f:
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            __version__ = line.split(delim)[1]
assert __version__ is not None, 'Unable to determine version'

setup(
    name='pykusto',
    version=__version__,
    packages=find_packages(exclude=['test']),
    url='https://github.com/Azure/pykusto',
    license='MIT License',
    author='Microsoft Corporation',
    author_email='yomost@microsoft.com',
    description='Advanced python SDK for Azure Data Explorer',
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    keywords="kusto azure-data-explorer client library query",
    install_requires=[
        # Release notes: https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/core/azure-core/CHANGELOG.md
        'azure-core==1.13.0',

        # Release notes: https://github.com/Azure/azure-kusto-python/releases
        'azure-kusto-data==2.1.1',  # Earlier versions not supported because of: https://github.com/Azure/azure-kusto-python/issues/312)

        # Release notes: https://pandas.pydata.org/docs/whatsnew/index.html
        'pandas>=0.25.0,<=1.2.4',  # Tests use DataFrame constructor options introduced in 0.25.0)

        'redo==2.0.4',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'flake8',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development",        
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
    ],
)
