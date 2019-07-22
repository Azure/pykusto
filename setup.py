from setuptools import setup

setup(
    name='pykusto',
    version='0.0.1',
    packages=[],
    url='https://dev.azure.com/yomost/_git/pykusto',
    license='MIT License',
    author='yomost',
    author_email='yomost@microsoft',
    description='Advanced python SDK for Azure Data Explorer',
    install_requires=['azure-kusto-data==0.0.31'],
)
