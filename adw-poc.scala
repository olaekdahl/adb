// Databricks notebook source


// COMMAND ----------

// MAGIC %scala
// MAGIC //https://www.mssqltips.com/sqlservertip/6151/using-azure-databricks-to-query-azure-sql-database/
// MAGIC 
// MAGIC val jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = "adw-user")
// MAGIC val jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "adw-password")
// MAGIC  
// MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------

dbutils.widgets.text("hostName", "", "power-bi-demo.database.windows.net")
dbutils.widgets.text("database", "", "AdventureWorksLT")

// COMMAND ----------

// MAGIC %scala
// MAGIC val jdbcHostname = dbutils.widgets.get("hostName")
// MAGIC val jdbcPort = 1433
// MAGIC val jdbcDatabase = dbutils.widgets.get("database")
// MAGIC  
// MAGIC // Create the JDBC URL without passing in the user and password parameters.
// MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
// MAGIC  
// MAGIC // Create a Properties() object to hold the parameters.
// MAGIC import java.util.Properties
// MAGIC val connectionProperties = new Properties()
// MAGIC  
// MAGIC connectionProperties.put("user", s"${jdbcUsername}")
// MAGIC connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

// MAGIC %scala
// MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
// MAGIC connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// MAGIC %scala
// MAGIC val saleslt_customer = spark.read.jdbc(jdbcUrl, "SalesLT.Customer", connectionProperties)

// COMMAND ----------

// MAGIC %scala
// MAGIC saleslt_customer.createOrReplaceTempView("Customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM Customers

// COMMAND ----------

// MAGIC %scala
// MAGIC var storage_key = dbutils.secrets.get(scope = "key-vault-secrets", key = "asadatalakej2dxbgn-key")

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.conf.set(
// MAGIC "fs.azure.account.key.asadatalakej2dxbgn.dfs.core.windows.net",
// MAGIC s"${storage_key}"
// MAGIC )

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.fs.ls("abfss://wwi-02@asadatalakej2dxbgn.dfs.core.windows.net/customer-info")

// COMMAND ----------

// MAGIC %scala
// MAGIC // set the data lake file location:
// MAGIC var file_location = "abfss://wwi-02@asadatalakej2dxbgn.dfs.core.windows.net/customer-info/customerinfo.csv"
// MAGIC  
// MAGIC // read in the data to dataframe df
// MAGIC var df = spark.read.format("csv").option("inferSchema", "true").option("header",
// MAGIC "true").option("delimiter",",").load(file_location)
// MAGIC  
// MAGIC // display the dataframe
// MAGIC display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- HIVE db
// MAGIC CREATE DATABASE adls_customers

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS adls_customers.customer_info
// MAGIC USING CSV
// MAGIC LOCATION 'abfss://wwi-02@asadatalakej2dxbgn.dfs.core.windows.net/customer-info/customerinfo.csv'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM adls_customers.customer_info;

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE adls_customers.customer_info;
// MAGIC CREATE TABLE IF NOT EXISTS adls_customers.customer_info
// MAGIC USING CSV
// MAGIC LOCATION 'abfss://wwi-02@asadatalakej2dxbgn.dfs.core.windows.net/customer-info/customerinfo.csv'
// MAGIC OPTIONS (header "true", inferSchema "true");

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM adls_customers.customer_info;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT db.FirstName, db.LastName, csv.CreditCard, csv.Email
// MAGIC FROM Customers AS db INNER JOIN adls_customers.customer_info AS csv ON db.EmailAddress = TRIM(csv.Email);

// COMMAND ----------


