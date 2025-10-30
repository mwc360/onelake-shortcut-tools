# onelake-shortcut-tools
[![PyPI Release](https://img.shields.io/pypi/v/onelake-shortcut-tools)](https://pypi.org/project/onelake-shortcut-tools/)
[![PyPI Downloads](https://img.shields.io/pepy/dt/onelake-shortcut-tools.svg?label=PyPI%20Downloads)](https://pypi.org/project/onelake-shortcut-tools/)

OneLake Shortcut Tools is library to help enable using OneLake Shortcuts in Microsoft Fabric. 
## Key Features:
- **Databricks Delta Table Compatibility Checker:** Quickly determine the compatibility of Databricks Delta tables with different Fabric runtime versions. Classification of read vs. write compatibility and whether or not droppable features exist which can allow for the table to be read or written to via Fabric Spark.
  
## Installation
```python
pip install onelake-shortcut-tools
```

## Usage
```python
from onelake_shortcut_tools.compatibility_checker import CompatibilityChecker

df = CompatibilityChecker(
    catalog_names=['catalog1', 'catalog2'], 
    schema_names=[], 
    fabric_runtime='1.3'
).evaluate()

display(df)
```
![image](https://github.com/mwc360/onelake-shortcut-tools/assets/52209784/ab31a52d-73e0-4e68-bf94-c499c474d7bf)

## Supported Configrations
- Databricks Unity Catalog is currently required. `hive_metastore` support will be added if there's interest. Please submit and issue!
- Apache Spark Runtime for Fabric 1.2, 1.3, and 2.0 EPP are currently supported.