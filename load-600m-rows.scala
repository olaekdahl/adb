// Databricks notebook source
import java.sql.{Connection, DriverManager}
import java.util.Properties

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = "adw-user")
val jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "adw-password")
 
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// hostname: power-bi-demo.database.windows.net
// database: AdventureWorksLT
dbutils.widgets.text("hostName", "", "Host name")
dbutils.widgets.text("database", "", "Database")

val jdbcHostname = dbutils.widgets.get("hostName")
val jdbcPort = 1433
val jdbcDatabase = dbutils.widgets.get("database")
 
// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
 
// Create a Properties() object to hold the parameters.
val connectionProperties = new Properties()
 
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

val conn = DriverManager.getConnection(jdbcUrl, connectionProperties)
val rs = conn.createStatement.executeQuery("exec dbo.GenerateCustomerData")
val data = Iterator.continually((rs.next(), rs)).takeWhile(_._1).map({case (_,rs) => rs.getString("FirstName") -> rs.getString("LastName")}).toList
val customersXL = sc.parallelize(data).toDF()
customersXL.createOrReplaceTempView("CustomersXL")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) 
// MAGIC FROM CustomersXL

// COMMAND ----------


