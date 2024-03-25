from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor

class CompatibilityChecker:
    """
    A class to check the compatibility of Delta tables with specified fabric runtime versions.
    
    Attributes:
        fabric_runtime: The version of the fabric runtime to check compatibility against.
        catalog_names: List of catalog names to be evaluated.
        schema_names: List of schema names to be evaluated. If None, all schemas are considered.    
    Methods:
        evaluate() -> DataFrame:
            Evaluates and returns a DataFrame summarizing the compatibility of tables within the specified catalogs and schemas.
    """
    def __init__(self, catalog_names: list[str], schema_names: list[str] = None, fabric_runtime: str = '1.2'):
        self.fabric_runtime = fabric_runtime
        self.catalog_names = catalog_names
        self.schema_names = schema_names
        self.spark = SparkSession.builder.appName("Compatibility Checker").getOrCreate()
        self._set_runtime_features()
        self.droppable_features = ['v2Checkpoint', 'deletionVectors']

        if 'hive_metastore' in catalog_names:
            raise ValueError(f"hive_metadata is not supported in this beta release. Please only reference Unity Catalog catalogs!")

    def _set_runtime_features(self):
        """
        Sets the valid writer and reader features based on the passed in fabric_runtime version.
        """
        match self.fabric_runtime:
            case '1.2':
                self.valid_fabric_writer_features = ['appendOnly', 'invariants', 'checkConstraints', 'generatedColumns', 'changeDataFeed', 'columnMapping', 'deletionVectors', 'timestampNtz']
                self.valid_fabric_reader_features = ['columnMapping', 'deletionVectors', 'timestampNtz']
            case '1.3':
                self.valid_fabric_writer_features = ['appendOnly', 'invariants', 'checkConstraints', 'generatedColumns', 'changeDataFeed', 'columnMapping', 'deletionVectors', 'timestampNtz', 'v2Checkpoint', 'domainMetadata']
                self.valid_fabric_reader_features = ['columnMapping', 'deletionVectors', 'timestampNtz', 'v2Checkpoint', 'domainMetadata']
            case _:
                raise ValueError(f"Fabric Runtime ({self.fabric_runtime}) is not supported for evaluation by this tool")
            
    def _get_table_features(self, catalog_name, schema_name, table_name):
        valid_writer_features = ['appendOnly', 'invariants', 'checkConstraints', 'generatedColumns', 'allowColumnDefaults', 'changeDataFeed', 'columnMapping', 'identityColumns', 'delectionVectors', 'rowTracking', 'timestampNtz', 'domainMetadata', 'v2Checkpoint', 'icebergCompatV1', 'liquid', 'clustering']

        valid_reader_features = ['columnMapping', 'deletionVectors', 'timestampNtz', 'v2Checkpoint']

        self.spark.catalog.setCurrentCatalog(catalog_name)
        table_details = self.spark.sql(f"DESCRIBE DETAIL {schema_name}.{table_name}").collect()[0]
        location = table_details['location']
        table_features = table_details['tableFeatures']
        min_reader = table_details['minReaderVersion']
        min_writer = table_details['minWriterVersion']

        reader_features = [feature for feature in table_features if feature in valid_reader_features]
        writer_features = [feature for feature in table_features if feature in valid_writer_features]
        min_table_protocol = {'minReaderVersion': min_reader, 'minWriterVersion': min_writer}
        return reader_features, writer_features, location, min_table_protocol
    
    def _evaluate_table(self, row):
        reader_features, writer_features, location, min_table_protocol = self._get_table_features(row['catalog'], row['schema'], row['table'])
        read_support = all(feature in self.valid_fabric_reader_features for feature in reader_features)
        write_support = all(feature in self.valid_fabric_writer_features for feature in writer_features)
        read_support_after_dropping_features = all(feature in self.valid_fabric_reader_features + self.droppable_features for feature in reader_features)
        write_support_after_dropping_features = all(feature in self.valid_fabric_writer_features + self.droppable_features for feature in writer_features)
        blocking_reader_features = [feature for feature in reader_features if feature not in self.valid_fabric_reader_features]
        blocking_writer_features = [feature for feature in writer_features if feature not in self.valid_fabric_writer_features]

        row['read_from_fabric'] = read_support
        row['write_from_fabric'] = write_support
        row['reader_features'] = reader_features
        row['writer_features'] = writer_features
        row['read_after_dropping_features'] = read_support_after_dropping_features if read_support == False else None
        row['write_after_dropping_features'] = write_support_after_dropping_features if write_support == False else None
        row['blocking_reader_features'] = blocking_reader_features
        row['blocking_writer_features'] = blocking_writer_features
        row['min_table_protocol'] = min_table_protocol
        row['location'] = location
            
    def evaluate(self) -> DataFrame:
        """
        Triggers the evaluation of compatilibity of tables based on the defined catalogs and schemas.
        """
        table_metadata = []
        for catalog in self.catalog_names:
            schemas = [row.databaseName for row in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect() if row.databaseName in self.schema_names or len(self.schema_names) == 0]
            for schema in schemas:
                tables = self.spark.sql(f"SHOW TABLES IN {schema}").collect()
                views = self.spark.sql(f"SHOW VIEWS IN {schema}").collect()
                view_list = [row.viewName for row in views if row.isTemporary == False]
                table_list = [row.tableName for row in tables if row.isTemporary == False and row.tableName not in view_list]

                for table in table_list:
                    table_metadata.append(
                        {
                            'full_table_reference': f"{catalog}.{schema}.{table}",
                            'catalog': catalog,
                            'schema': schema,
                            'table': table,
                        }
                    )

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._evaluate_table, row) for row in table_metadata]

        schema = StructType([
            StructField('full_table_reference', StringType(), True),
            StructField('catalog', StringType(), True),
            StructField('schema', StringType(), True),
            StructField('table', StringType(), True),
            StructField('reader_features', ArrayType(StringType()), True),
            StructField('writer_features', ArrayType(StringType()), True),
            StructField('read_from_fabric', BooleanType(), True),
            StructField('write_from_fabric', BooleanType(), True),
            StructField('read_after_dropping_features', BooleanType(), True),
            StructField('write_after_dropping_features', BooleanType(), True),
            StructField('blocking_reader_features', ArrayType(StringType()), True),
            StructField('blocking_writer_features', ArrayType(StringType()), True),
            StructField('min_table_protocol', StructType([
                StructField('minReaderVersion', StringType()),
                StructField('minWriterVersion', StringType())
            ]), True),  
            StructField('location', StringType(), True)

        ])
        
        spark_df = self.spark.createDataFrame(table_metadata, schema)
        spark_df = spark_df.select(
            'catalog', 
            'schema', 
            'table', 
            'read_from_fabric', 
            'write_from_fabric', 
            'reader_features', 
            'writer_features', 
            'blocking_reader_features', 
            'blocking_writer_features', 
            'read_after_dropping_features', 
            'write_after_dropping_features', 
            'min_table_protocol', 
            'full_table_reference', 
            'location'
        )

        return spark_df
