from setuptools import setup, find_packages

setup(
    name='pykusto',
    version='0.0.17',
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
        'azure-kusto-data>=0.0.43,<=0.0.44',  # In 0.0.43 the 'azure.kusto.data.response' package was renamed
        'pandas>=0.24.1,<=1.0.3',  # azure-kusto-data requires 0.24.1
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'flake8',
        'pandas>=0.25.0',  # Tests use DataFrame constructor options introduced in 0.25.0
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
