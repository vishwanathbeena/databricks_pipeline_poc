from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

import json
import logging

from metastore_operations import DBMetastoreOperations

logger = logging.getLogger("databricks-pipeline")
logger.setLevel(logging.INFO)

class LoadControlTable:
    
    def __init__(self,spark,args_dict):
        self.spark = spark
        self.db_metastore = DBMetastoreOperations(self.spark)
        self.args_dict = args_dict

    def load_control_table(self,args_dict):
        self.db_metastore.create_database(args_dict['control_database'])
        self.__load_raw_control_table()
        self.__load_intg_control_table()
        
    
    def __load_raw_control_table(self):
        control_df = self.__get_control_file_dataframe(self.args_dict["control_file_path"])
        raw_dataflow_spec_df = self.__get_raw_dataflow_spec_dataframe(control_df, self.args_dict["env"])
        raw_dataflow_spec_df.write.mode("overwrite").format("parquet").save(path=self.args_dict["control_flow_table_path_raw"])
        self.register_raw_control_flow_tables()
    
    def __load_intg_control_table(self):
        control_df = self.__get_control_file_dataframe(self.args_dict["control_file_path"])
        intg_dataflow_spec_df = self.__get_intg_dataflow_spec_dataframe(control_df, self.args_dict["env"])
        intg_dataflow_spec_df.write.mode("overwrite").format("parquet").save(path=self.args_dict["control_flow_table_path_intg"])
        self.register_intg_control_flow_tables()


    def __get_control_file_dataframe(self, onboarding_file_path):
        control_df = None
        if onboarding_file_path.lower().endswith(".json"):
            control_df = self.spark.read.option("multiline", "true").json(onboarding_file_path)
            self.onboard_file_type = "json"
            control_df_dupes = control_df.groupBy("data_flow_id").count().filter("count > 1")
            if len(control_df_dupes.head(1)) > 0:
                control_df_dupes.show()
                raise Exception("onboarding file have duplicated data_flow_ids! ")
        else:
            raise Exception("Onboarding file format not supported! Please provide json file format")
        return control_df
    
    def __get_raw_dataflow_spec_dataframe(self, control_df, env):
        """Get raw dataflow spec method will convert onboarding dataframe to raw Dataflowspec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "writerConfigOptions",
            "schema",
            "partitionColumns",
            "cdcApplyChanges"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField("sourceDetails", MapType(StringType(), StringType(), True), True),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField("targetDetails", MapType(StringType(), StringType(), True), True),
                StructField(
                    "writerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("schema", StringType(), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True)
            ]
        )
        data = []
        onboarding_rows = control_df.collect()
        for onboarding_row in onboarding_rows:
            raw_data_flow_spec_id = onboarding_row["data_flow_id"]
            raw_data_flow_spec_group = onboarding_row["data_flow_group"]
            if "source_format" not in onboarding_row:
                raise Exception(f"Source format not provided for row={onboarding_row}")

            source_format = onboarding_row["source_format"]
            if source_format.lower() not in ["csv", "parquet", "json"]:
                raise Exception(f"Source format {source_format} not supported in DLT-META! row={onboarding_row}")
            source_details = {}
            raw_reader_config_options = {}
            schema = None
            raw_reader_options_json = onboarding_row["raw_reader_options"]
            if raw_reader_options_json:
                raw_reader_config_options = raw_reader_options_json.asDict()
            source_details_json = onboarding_row["source_details"]
            if source_details_json:
                source_details_file = source_details_json.asDict()
                if source_format.lower() in ["csv", "parquet", "json"]:
                    if f"source_path_{env}" in source_details_file:
                        source_details["path"] = source_details_file[f"source_path_{env}"]
                    if "source_database" in source_details_file:
                        source_details["source_database"] = source_details_file["source_database"]
                    if "source_table" in source_details_file:
                        source_details["source_table"] = source_details_file["source_table"]
                elif source_format.lower() == "eventhub" or source_format.lower() == "kafka":
                    source_details = source_details_file
                if "source_schema_path" in source_details:
                    source_schema_path = source_details["source_schema_path"]
                    if source_schema_path:
                        if self.raw_schema_mapper is not None:
                            schema = self.raw_schema_mapper(source_schema_path, self.spark)
                        else:
                            schema = self.__get_raw_schema(source_schema_path)
                    else:
                        logger.info(f"no input schema provided for row={onboarding_row}")

                logger.info("spark_schmea={}".format(schema))

            raw_target_format = "parquet"

            raw_target_details = {
                "database": onboarding_row["raw_database_{}".format(env)],
                "table": onboarding_row["raw_table"],
                "path": onboarding_row["raw_table_path_{}".format(env)],
            }

            raw_writer_config_options = {}

            partition_columns = [""]
            if "raw_partition_columns" in onboarding_row and onboarding_row["raw_partition_columns"]:
                partition_columns = [onboarding_row["raw_partition_columns"]]

            cdc_apply_changes = None
            if "raw_cdc_apply_changes" in onboarding_row and onboarding_row["raw_cdc_apply_changes"]:
                self.__validateApplyChanges(onboarding_row, "raw")
                cdc_apply_changes = json.dumps(onboarding_row["raw_cdc_apply_changes"].asDict())
            data_quality_expectations = None
            quarantine_target_details = {}
            if f"raw_data_quality_expectations_json_{env}" in onboarding_row:
                raw_data_quality_expectations_json = onboarding_row[f"raw_data_quality_expectations_json_{env}"]
                if raw_data_quality_expectations_json:
                    data_quality_expectations = (
                        self.__get_data_quality_expecations(raw_data_quality_expectations_json))
                    if onboarding_row["raw_quarantine_table"]:
                        quarantine_table_partition_columns = ""
                        if (
                            "raw_quarantine_table_partitions" in onboarding_row
                            and onboarding_row["raw_quarantine_table_partitions"]
                        ):
                            quarantine_table_partition_columns = onboarding_row["raw_quarantine_table_partitions"]
                        quarantine_target_details = {
                            "database": onboarding_row[f"raw_database_quarantine_{env}"],
                            "table": onboarding_row["raw_quarantine_table"],
                            "path": onboarding_row[f"raw_quarantine_table_path_{env}"],
                            "partition_columns": quarantine_table_partition_columns,
                        }
            raw_row = (
                raw_data_flow_spec_id,
                raw_data_flow_spec_group,
                source_format,
                source_details,
                raw_reader_config_options,
                raw_target_format,
                raw_target_details,
                raw_writer_config_options,
                schema,
                partition_columns,
                cdc_apply_changes
                # data_quality_expectations,
                # quarantine_target_details,
            )
            data.append(raw_row)
            # logger.info(raw_parition_columns)

        data_flow_spec_rows_df = self.spark.createDataFrame(data, data_flow_spec_schema).toDF(*data_flow_spec_columns)

        return data_flow_spec_rows_df

    def __get_intg_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get intg_dataflow_spec method transform onboarding dataframe to intg dataflowSpec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "writerConfigOptions",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField("sourceDetails", MapType(StringType(), StringType(), True), True),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField("targetDetails", MapType(StringType(), StringType(), True), True),
                StructField(
                    "writerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("schema", StringType(), True),                
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()

        for onboarding_row in onboarding_rows:
            intg_data_flow_spec_id = onboarding_row["data_flow_id"]
            intg_data_flow_spec_group = onboarding_row["data_flow_group"]
            intg_reader_config_options = {}

            intg_target_format = "parquet"

            raw_target_details = {
                "database": onboarding_row["raw_database_{}".format(env)],
                "table": onboarding_row["raw_table"],
                "path": onboarding_row["raw_table_path_{}".format(env)],
            }
            intg_target_details = {
                "database": onboarding_row["intg_database_{}".format(env)],
                "table": onboarding_row["intg_table"],
                "path": onboarding_row["intg_table_path_{}".format(env)],
            }

            intg_writer_config_options = {}
            schema = None

            intg_parition_columns = [onboarding_row["intg_partition_columns"]]  # TODO correct typo in tsv
            intg_cdc_apply_changes = None
            if "intg_cdc_apply_changes" in onboarding_row and onboarding_row["intg_cdc_apply_changes"]:
                self.__validateApplyChanges(onboarding_row, "intg")
                intg_cdc_apply_changes_row = onboarding_row["intg_cdc_apply_changes"]
                if self.onboard_file_type == "json":
                    intg_cdc_apply_changes = json.dumps(intg_cdc_apply_changes_row.asDict())
                elif self.onboard_file_type == "csv" and len(intg_cdc_apply_changes_row.strip()) > 0:
                    intg_cdc_apply_changes = json.loads(intg_cdc_apply_changes_row)

            intg_row = (
                intg_data_flow_spec_id,
                intg_data_flow_spec_group,
                intg_target_format,
                raw_target_details,
                intg_reader_config_options,
                intg_target_format,
                intg_target_details,
                intg_writer_config_options,
                schema,
                intg_parition_columns,
                intg_cdc_apply_changes,
            )
            data.append(intg_row)
            logger.info(f"intg_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(data, data_flow_spec_schema).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df    

    def register_raw_control_flow_tables(self):
        """Register control flow tables."""
        self.db_metastore.register_table_in_metastore(
            self.args_dict["control_database"],
            self.args_dict["control_flow_table_raw"],
            self.args_dict["control_flow_table_path_raw"],
        )
        logger.info(
            f"""onboarded control flow table={ self.args_dict["control_database"]}.{self.args_dict["control_flow_table_raw"]}"""
        )
        self.spark.read.table(f"""{ self.args_dict["control_database"]}.{self.args_dict["control_flow_table_raw"]}""").show(truncate=False)

    def register_intg_control_flow_tables(self):
        """Register control flow tables."""
        self.db_metastore.register_table_in_metastore(
            self.args_dict["control_database"],
            self.args_dict["control_flow_table_intg"],
            self.args_dict["control_flow_table_path_intg"],
        )
        logger.info(
            f"""onboarded control flow table={ self.args_dict["control_database"]}.{self.args_dict["control_flow_table_intg"]}"""
        )
        self.spark.read.table(f"""{ self.args_dict["control_database"]}.{self.args_dict["control_flow_table_intg"]}""").show(truncate=False)

