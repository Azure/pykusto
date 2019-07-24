# Introduction 
_pykusto_ is an advanced Python SDK for Azure Data Explorer (a.k.a. Kusto).

# Getting Started
### Installation
pip install git+https://yomost@dev.azure.com/yomost/pykusto/_git/pykusto

### Usage
```
from pykusto.tables import PyKustoClient
from pykusto.query import Query
from pykusto.column import column_generator as col

client = PyKustoClient('https://help.kusto.windows.net')
print(client.show_databases())

print(client['Samples'].show_tables())

table = client['Samples']['StormEvents']
Query(table)\
    .project(col.StartTime, col.EndTime, col.EventType, col.Source)\
    .extend(Duration=col.EndTime - col.StartTime)\
    .where(col.Duration > timedelta(hours=1))\
    .take(5)\
    .to_dataframe()
```