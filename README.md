# Introduction 
_pykusto_ is an advanced Python SDK for Azure Data Explorer (a.k.a. Kusto).  
Started as a project in the 2019 Microsoft Hackathon.

# Getting Started
### Installation
```bash
pip install pykusto
```

### Usage
```python
from datetime import timedelta
from pykusto.client import PyKustoClient
from pykusto.query import Query
from pykusto.expressions import column_generator as col

# Connect to cluster with AAD device authentication
client = PyKustoClient('https://help.kusto.windows.net')

# Show databases
print(client.show_databases())

# Show tables in 'Samples' database
print(client['Samples'].show_tables())

# Connect to 'StormEvents' table
table = client['Samples']['StormEvents']

# Build query
(
    Query(table)        
        # Access columns using 'col' global variable 
        .project(col.StartTime, col.EndTime, col.EventType, col.Source)
        # Specify new column name using Python keyword argument   
        .extend(Duration=col.EndTime - col.StartTime)
        # Python types are implicitly converted to Kusto types
        .where(col.Duration > timedelta(hours=1))
        .take(5)
        # Output to pandas dataframe
        .to_dataframe()
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
