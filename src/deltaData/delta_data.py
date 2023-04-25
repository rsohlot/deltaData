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

    def smart_append(spark, delta_table_path, new_data, id_columns=None):
        """
        A function that creates a smart append option on top of Delta Lake with Spark
        and finds duplicates based on the provided list of columns, or all columns if not provided.

        :param delta_table_path: The path to the directory where the Delta table is stored.
        :param new_data: The new data frame to append to the Delta table.
        :param id_columns: The list of columns to use as IDs when checking for duplicates. If not provided, all columns will be used.
        """
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
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(delta_table_path)
            return 

        # left_df and right_df are the two dataframes to join
        # id_columns is a list of columns to join on

        # perform left-outer join
        joined_df = delta_table.join(new_data, id_columns, "outer")

        # select values from right dataframe if they exist, and from left dataframe otherwise
        result_df = joined_df.select([coalesce(new_data[c], delta_table[c]).alias(c) for c in new_data.columns if c != "append_timestamp"]
                                    + [coalesce(delta_table["append_timestamp"], new_data["append_timestamp"]).alias("append_timestamp")])



        result_df \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(delta_table_path)