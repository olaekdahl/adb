// Databricks notebook source
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.current_timestamp 

// COMMAND ----------

var storage_key = dbutils.secrets.get(scope = "key-vault-secrets", key = "awstrg-key")
spark.conf.set(
  "fs.azure.account.key.awstrg.dfs.core.windows.net",
  s"${storage_key}"
)
var parquet_path = "abfss://files@awstrg.dfs.core.windows.net/"
var df = (spark.read.format("parquet").load(parquet_path).withColumn("type", lit("batch")).withColumn("timestamp", current_timestamp()))
df.write.format("delta").mode("overwrite").saveAsTable("customers_delta") //https://spark.apache.org/docs/2.3.1/sql-programming-guide.html#saving-to-persistent-tables

// COMMAND ----------

spark.sql("select count(*) as records_count from customers_delta").show()
spark.sql("select * from customers_delta").show(10)
spark.sql("select firstname, lastname, type, timestamp from customers_delta").show(10)

// COMMAND ----------


