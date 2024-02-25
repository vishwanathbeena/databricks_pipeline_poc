from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import logging
import json

logger = logging.getLogger("databricks-pipeline")
logger.setLevel(logging.INFO)

class DBMetastoreOperations:
    
    def __init__(self,spark):
        self.spark = spark
    
    def create_database(self,database):
        """Create Database"""
        self.try_run_sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    def try_run_sql(self, sql):
        """Try running SQL."""
        self.spark.sql(sql)
    
    def drop_database(self, database):
        """Drop database."""
        self.try_run_sql(f"DROP DATABASE IF EXISTS {database} ")

    def reset_table_in_metastore(self, database, table, path):
        """Reset table metadata."""
        self.deregister_table_from_metastore(database, table)
        self.register_table_in_metastore(database, table, path)
    
    def register_table_in_metastore(self, database, table, path):
        """Register table in metastore."""
        final_table_name = f"{database}.{table}"
        self.spark.sql("CREATE TABLE if not exists " + final_table_name + " USING PARQUET LOCATION '" + path + "'")


    def deregister_table_from_metastore(self, database, table):
        """Deregister table."""
        self.try_run_sql(f"DROP TABLE IF EXISTS {database}.{table}")

    def get_table_location(self, database, table):
        """Get table location on blobstorage."""
        return self.spark.sql(f"describe extended {database}.{table}").where("col_name='Location'").collect()[0][1]
