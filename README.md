# onelake-shortcut-tools
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
    fabric_runtime='1.2'
).evaluate()

display(df)
```
![image](https://github.com/mwc360/onelake-shortcut-tools/assets/52209784/ab31a52d-73e0-4e68-bf94-c499c474d7bf)

## Supported Configrations
- Databricks Unity Catalog is currently required for the beta release. Support for `hive_metastore` objects will be added in the next release.
- Apache Spark Runtime for Fabric 1.2 and 1.3 are currently supported.


