# Introduction 
_pykusto_ is an advanced Python SDK for Azure Data Explorer (a.k.a. Kusto).  
Started as a project in the 2019 Microsoft Hackathon.

[![PyPI version](https://badge.fury.io/py/pykusto.svg)](https://badge.fury.io/py/pykusto)
[![Downloads](https://pepy.tech/badge/pykusto)](https://pepy.tech/project/pykusto)

# Getting Started
### Installation
```bash
pip install pykusto
```

### Basic usage
```python
from datetime import timedelta
from pykusto import PyKustoClient, Query

# Connect to cluster with AAD device authentication
# Databases, tables, and columns are auto-retrieved
client = PyKustoClient('https://help.kusto.windows.net')

# Show databases
print(tuple(client.get_databases_names()))

# Show tables in 'Samples' database
print(tuple(client.Samples.get_table_names()))

# Connect to 'StormEvents' table
t = client.Samples.StormEvents

# Build query
(
    Query(t)        
        # Access columns using table variable 
        .project(t.StartTime, t.EndTime, t.EventType, t.Source)
        # Specify new column name using Python keyword argument   
        .extend(Duration=t.EndTime - t.StartTime)
        # Python types are implicitly converted to Kusto types
        .where(t.Duration > timedelta(hours=1))
        .take(5)
        # Output to pandas dataframe
        .to_dataframe()
) 
```

### Retrying failed queries
```python
# Turn on retrying for all queries 
client = PyKustoClient(
    'https://help.kusto.windows.net',
    retry_config=RetryConfig()  # Use default retry config 
)

# Override retry config for specific query 
Query(client.Samples.StormEvents).take(5).to_dataframe(
    retry_config=RetryConfig(attempts=3, sleep_time=1, max_sleep_time=600, sleep_scale=2, jitter=3)
)
```

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
