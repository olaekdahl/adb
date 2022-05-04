// Databricks notebook source
var storage_key = dbutils.secrets.get(scope = "key-vault-secrets", key = "asadatalakes4xt9l0-key")

spark.conf.set("fs.azure.account.key.asadatalakes4xt9l0.dfs.core.windows.net", s"${storage_key}")

// dbutils.fs.ls("abfss://wwi-02@asadatalakes4xt9l0.dfs.core.windows.net/customer-info")

// set the data lake file location:
var file_location = "abfss://wwi-02@asadatalakes4xt9l0.dfs.core.windows.net/customer-info/customerinfo.csv"
 
// read in the data to dataframe df
var df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
// display the dataframe
display(df)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val csvSchema = StructType(
  StructField("UserName", StringType, false) ::
  StructField("Gender", StringType, false) ::
  StructField("Phone", StringType, false) ::
  StructField("Email", StringType, false) ::
  StructField("CreditCard", StringType, false) :: Nil)

// COMMAND ----------

var df = spark.read.format("csv").option("header", "true").option("delimiter",",").schema(csvSchema).load(file_location)
 
// display the dataframe
display(df)

// COMMAND ----------

df.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from customers

// COMMAND ----------

// MAGIC %sql 
// MAGIC select Gender, count(*) as cnt
// MAGIC from customers
// MAGIC group by Gender

// COMMAND ----------

val gendersDF = df.sqlContext.sql("select Gender, count(*) as cnt from customers group by Gender")

// COMMAND ----------

display(gendersDF)

// COMMAND ----------

// var file_dest = "abfss://wwi-02@asadatalakes4xt9l0.dfs.core.windows.net/customer-info/genders_count.parquet"
// gendersDF.write.mode("overwrite").parquet(file_dest)

// var file_dest = "abfss://wwi-02@asadatalakes4xt9l0.dfs.core.windows.net/customer-info/genders_count2.parquet"
// gendersDF.write.parquet(file_dest)

var file_dest = "abfss://wwi-02@asadatalakes4xt9l0.dfs.core.windows.net/customer-info/genders_count3.csv"
gendersDF.write.csv(file_dest)

// COMMAND ----------

import org.apache.spark.sql.functions._
df.select(col("Email")).show()

// COMMAND ----------

df.filter($"Gender" === "Male").show()

// COMMAND ----------

df.filter($"Gender" === "Male").sort($"UserName".asc).show()

// COMMAND ----------

val groupedDF = df.select($"Gender", $"UserName").groupBy($"Gender").agg(countDistinct($"UserName") as "UserCount")
display(groupedDF)
