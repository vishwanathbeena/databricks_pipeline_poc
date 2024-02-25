import logging
import json 
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col
from pipelineflow_spec import DataflowSpec

logger = logging.getLogger("databricks-pipeline")
logger.setLevel(logging.INFO)

class SparkUtils():

    @staticmethod
    def read_files(spark,dataflowspec:DataflowSpec) -> DataFrame :
        source_path = dataflowspec.sourceDetails['path']
        read_options = dataflowspec.readerConfigOptions
        if dataflowspec.schema:
            schema_json = json.loads(dataflowspec.schema)
            schema = StructType.fromJson(schema_json)
            return (spark.read.format(dataflowspec.sourceFormat).options(**read_options).schema(schema).load(source_path))
        else:
            return (spark.read.format(dataflowspec.sourceFormat).options(**read_options).load(source_path))
    
    @staticmethod
    def write_files(spark:SparkSession,dataflowspec:DataflowSpec,dataframe:DataFrame):
        target_path = dataflowspec.targetDetails['path']
        dataframe.write.mode('overwrite').format(dataflowspec.targetFormat).save(path=target_path)