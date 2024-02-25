import logging
from typing import List
from pipelineflow_spec import DataflowSpec
from pyspark.sql import DataFrame
from spark_utils import SparkUtils
from metastore_operations import DBMetastoreOperations

logger = logging.getLogger("databricks-pipeline")
logger.setLevel(logging.INFO)

class DataFlow():
    def __init__(self,spark,args_dict,data_flow_spec:DataflowSpec):
        self.spark=spark
        self.args_dict = args_dict
        self.dataflow_spec = data_flow_spec
        self.dataframe:DataFrame = None
        self.dbmetastore = DBMetastoreOperations(self.spark)
    
    def run_pipeline(self,):
        self.__read()
        self.__write()
        self.__register_table()
    
    def __read(self):
        self.dataframe = SparkUtils.read_files(self.spark,self.dataflow_spec)

    
    def __write(self):
        SparkUtils.write_files(self.spark,self.dataflow_spec,self.dataframe)
        self.spark.read.format('parquet').load(self.dataflow_spec.targetDetails['path']).show(truncate=False)
    
    def __register_table(self):
        self.dbmetastore.create_database(self.dataflow_spec.targetDetails['database'])
        # self.dbmetastore.register_table_in_metastore(self.dataflow_spec.targetDetails['database'],self.dataflow_spec.targetDetails['table'],self.dataflow_spec.targetDetails['path'])
        self.dbmetastore.reset_table_in_metastore(self.dataflow_spec.targetDetails['database'],self.dataflow_spec.targetDetails['table'],self.dataflow_spec.targetDetails['path'])
        logger.info(f" Table {self.dataflow_spec.targetDetails['database']}.{self.dataflow_spec.targetDetails['table']} has been registered")
        qry = f" select * from {self.dataflow_spec.targetDetails['database']}.{self.dataflow_spec.targetDetails['table']}"
        print(f"printing results of {self.dataflow_spec.targetDetails['database']}.{self.dataflow_spec.targetDetails['table']}")
        print(qry)
        
        self.spark.sql(f"DESCRIBE TABLE EXTENDED {self.dataflow_spec.targetDetails['database']}.{self.dataflow_spec.targetDetails['table']}").show(truncate=False)
        # self.spark.sql(qry).show(truncate=False)
        self.spark.read.table(f"{self.dataflow_spec.targetDetails['database']}.{self.dataflow_spec.targetDetails['table']}").show(truncate=False)


        
        
    

class DataFlowOps():

    def __init__(self,spark,args_dict):
        self.spark = spark
        self.args_dict = args_dict

    def invoke_pipelines(self):
        raw_pipeline_rows = self.__get_raw_pipeline()
        self.process_pipelines(raw_pipeline_rows)
        intg_pipeline_rows = self.__get_intg_pipeline()
        self.process_pipelines(intg_pipeline_rows)
        
    
    def process_pipelines(self,pipeline_rows:List[DataflowSpec]):
        for dataflowspec in pipeline_rows:
            dataflowobj = DataFlow(self.spark,self.args_dict,dataflowspec)
            dataflowobj.run_pipeline()

    
    def __get_raw_pipeline(self):
        table_name = self.args_dict['control_flow_table_raw']
        raw_df_rows = self.__get_df(table_name).collect()
        raw_control_records:list[DataflowSpec] = []
        for row in raw_df_rows:
            raw_control_records.append(DataflowSpec(**row.asDict()))
        return raw_control_records
    
    def __get_intg_pipeline(self):
        table_name = self.args_dict['control_flow_table_intg']
        intg_df_rows = self.__get_df(table_name).collect()
        intg_control_records:list[DataflowSpec] = []
        for row in intg_df_rows:
            intg_control_records.append(DataflowSpec(**row.asDict()))
        return intg_control_records
    
    def __get_df(self,table_name):
        raw_control_table = self.args_dict['control_database']+'.'+table_name
        raw_pipeline_df  = self.spark.read.table(raw_control_table)
        return raw_pipeline_df
        


