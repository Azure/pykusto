# Introduction 
_pykusto_ is an advanced Python SDK for Azure Data Explorer (a.k.a. Kusto).

# Getting Started
### Installation
```bash
pip install git+https://yomost@dev.azure.com/yomost/pykusto/_git/pykusto
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
        # Determine new column name using Python keyword argument   
        .extend(Duration=col.EndTime - col.StartTime)
        # Python types are implicitly converted to Kusto types behind the scenes
        .where(col.Duration > timedelta(hours=1))
        .take(5)
        # Output to pandas dataframe
        .to_dataframe()
) 
```
