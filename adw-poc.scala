// Databricks notebook source
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
// MAGIC // val application_countries = spark.read.jdbc(jdbcUrl, "Application.Countries", connectionProperties)
// MAGIC // val application_stateprovinces = spark.read.jdbc(jdbcUrl, "Application.StateProvinces", connectionProperties)

// COMMAND ----------

// MAGIC %scala
// MAGIC saleslt_customer.createOrReplaceTempView("Customers")
// MAGIC // application_countries.createOrReplaceTempView("Countries")
// MAGIC // application_stateprovinces.createOrReplaceTempView("StateProvinces")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM Customers
// MAGIC 
// MAGIC --  %sql
// MAGIC --  Select CityID,CityName,StateProvinceCode,SalesTerritory,CountryName,CountryType,Continent,Region,Subregion 
// MAGIC --  FROM Cities c 
// MAGIC --  INNER JOIN StateProvinces sp ON c.StateProvinceID = sp.StateProvinceID 
// MAGIC --  INNER JOIN Countries co ON sp.CountryID = co.CountryID 
// MAGIC --  LIMIT 10

// COMMAND ----------


