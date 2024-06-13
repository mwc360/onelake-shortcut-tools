# Databricks notebook source
from compatibility_checker import *

# COMMAND ----------

df = CompatibilityChecker(
    catalog_names=[''], 
    schema_names=[], 
    fabric_runtime='1.3'
).evaluate()

display(df)
