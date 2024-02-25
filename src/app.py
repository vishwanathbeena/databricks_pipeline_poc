import logging
import argparse
import json
from pyspark.sql import SparkSession
from control_file_operations import LoadControlTable
from dataflow_operations import DataFlowOps

logger = logging.getLogger("databricks-pipeline")
logger.setLevel(logging.INFO)


arguments = ["--reload_control_flow",
             "--control_file_path",
             "--control_metadata_path",
             "--env",
             "--overwrite",
             "--local_mode"
             ]

def parse_args():
    """Parse command line."""
    parser = argparse.ArgumentParser()
    for argument in arguments:
        parser.add_argument(argument)
    args = parser.parse_args()
    logger.info(f"Input arguments dict: {args}")
    return args

def _get_spark_session():
    return (SparkSession.builder.appName("databricks_pipeline")).getOrCreate()

def main():
    args = parse_args()
    args_dict = args.__dict__
    control_metadata = json.load(open(args.control_metadata_path))
    args_dict['table_control'] = args.control_file_path
    args_dict.update(control_metadata)
    spark = _get_spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    load_ct= LoadControlTable(spark,args_dict)
    dataflow = DataFlowOps(spark,args_dict)

    if args.local_mode == 'y':
        spark.sparkContext.setSystemProperty("hadoop.home.dir","C:/hadoop/bin")
    
    if args.reload_control_flow.lower() == 'y':
        load_ct.load_control_table(args_dict)
    
    dataflow.invoke_pipelines()


if __name__ == '__main__':
    main()