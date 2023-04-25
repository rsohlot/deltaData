from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, coalesce
import os
import pyspark
from delta import *
from pyspark.sql.functions import col
from pyspark.sql.functions import when, lit
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql.functions import col


class deltaData:
    def __init__(self) -> None:
        self.spark = self.get_spark_session()


    def get_spark_session(self):
        """
        Create spark session for the class function usage.
        """
        if self.spark:
            return self.spark
        else:
            builder = pyspark.sql.SparkSession.builder.appName("DeltaHouse") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            return self.spark

    def get_delta_data(self, delta_table_path):
        """
        Return the data from the lake.
        """
        df = self.spark.read.format("delta").load(delta_table_path)
        return df
  
    def smart_append(self, spark, new_data, delta_table_path, id_cols, partition_by):
        """
        Add data to delta lake in a manner if there is already data exist for same id list combination it will overwrite else it will insert new record.
        """

        # Add timestamp column to data
        new_data = new_data.withColumn("append_timestamp", current_timestamp()).withColumn("update_timestamp", current_timestamp())
        
        # check if data lake present at the path or not
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


    def compare_versions(self, spark , delta_lake_path, id_cols, version1, version2):
        """
        Compare the diff in 2 versions, if no versions are provieded , it will pick the latest and second last version to compare
        """
        deltaTable = DeltaTable.forPath(spark, delta_lake_path)

        if not version1 or version2:
            data_history = deltaTable.history()
            versions = data_history.select("version").collect()
            versions = [v.asDict().get("version") for v in versions]
            versions = sorted(versions)
            last_version = versions[-1]
            second_last_version = versions[-2]

        else:
            last_version = version2
            second_last_version = version1

        delta_table_last_version = spark.read.format("delta").option("versionAsOf", f"{last_version}").load(delta_lake_path)
        delta_table_second_last_version = spark.read.format("delta").option("versionAsOf", f"{second_last_version}").load(delta_lake_path)

        join_condition = reduce(lambda x, y: x & y, [col(f"a.{id_col}") == col(f"b.{id_col}") for id_col in id_cols])

        joined_df = delta_table_second_last_version.alias("a").join(delta_table_last_version.alias("b"), join_condition, "outer")

        # Define a condition to check if all columns are equal
        other_columns = [col_name for col_name in delta_table_second_last_version.columns if col_name not in id_cols and col_name != "update_timestamp"]
        condition = " OR ".join([f"(a.{col_name} IS NOT NULL AND b.{col_name} IS NOT NULL AND a.{col_name} != b.{col_name})" for col_name in other_columns])

        # Filter the joined DataFrame using the condition
        differences_df = joined_df.filter(condition)

        # Create a new DataFrame with the differences
        # diff_df = joined_df.select(*[col(f"a.{id_col}").alias(id_col) for id_col in id_cols], 
        #                            *[when(col(f"a.{col_name}") != col(f"b.{col_name}"), 
        #                                   concat(lit("old: "), col(f"a.{col_name}"), lit(" | new: "), col(f"b.{col_name}")))
        #                              .otherwise(None).alias(col_name) for col_name in other_columns])

        diff_df = joined_df.select(*[col(f"a.{id_col}").alias(id_col) for id_col in id_cols], 
                                *[when(col(f"a.{col_name}") != col(f"b.{col_name}"), 
                                        concat(lit(f"version {second_last_version}: "), col(f"a.{col_name}"), lit(f" | version {last_version}: "), col(f"b.{col_name}")))
                                    .otherwise(None).alias(col_name) for col_name in other_columns])

        # Define a condition to check if at least one column has a difference
        condition = " OR ".join([f"{col_name} IS NOT NULL" for col_name in other_columns])

        # Filter the DataFrame using the condition
        differences_df = diff_df.filter(condition)


        print("Diff:")
        print(differences_df.show(truncate=False))

        return differences_df