from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, coalesce
import os
import pyspark
from delta import *


class deltaData:
    def __init__(self) -> None:
        self.spark = self.get_spark_session()


    def get_spark_session(self):
        if self.spark:
            return self.spark
        else:
            builder = pyspark.sql.SparkSession.builder.appName("DeltaHouse") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            return self.spark

    def get_delta_data(self, delta_table_path):
        df = self.spark.read.format("delta").load(delta_table_path)
        return df
  
    def smart_append(self, spark, new_data, delta_table_path, id_cols, partition_by):
        new_data = new_data.withColumn("append_timestamp", current_timestamp()).withColumn("update_timestamp", current_timestamp())
        
        if os.path.exists(delta_table_path):
            # Read the existing Delta table
            delta_table = spark.read.format("delta").load(delta_table_path)
        else:
            # Write the new data as the initial Delta table
            new_data \
                .withColumn("append_timestamp", current_timestamp()) \
                .write \
                .format("delta") \
                .partitionBy(*partition_by) \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(delta_table_path)
            return

        df_schema = new_data.schema

        # Get the schema field names excluding the unique key and the column you don't want to update
        columns_to_update = [field.name for field in df_schema.fields if field.name not in {*id_cols, "append_timestamp"}]

        # Generate the update column dictionary
        update_dict = {column: f"source.{column}" for column in columns_to_update}

        id_merge = " AND ".join([f"target.{column} = source.{column}" for column in id_cols])
        deltaTable = DeltaTable.forPath(spark, delta_table_path)
        # Perform the upsert
        deltaTable.alias("target").merge(
            new_data.alias("source"),
            id_merge
        ).whenMatchedUpdate(
            set=update_dict
        ).whenNotMatchedInsertAll().execute()