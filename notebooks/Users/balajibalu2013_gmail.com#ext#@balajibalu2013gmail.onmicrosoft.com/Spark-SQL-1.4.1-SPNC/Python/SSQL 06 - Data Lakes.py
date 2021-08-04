# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Data Lakes with SQL
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; make it easy to work with hierarchical data, such as nested JSON records.
# MAGIC 
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a Data Lake.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use SQL to query a Data Lake
# MAGIC * Clean messy data sets
# MAGIC * Join two cleaned data sets
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Analysts
# MAGIC * Additional Audiences: Data Engineers and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.3**
# MAGIC * Familiarity with <a href="https://www.w3schools.com/sql/" target="_blank">ANSI SQL</a> is required

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup & Classroom-Cleanup<br>
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lakes
# MAGIC 
# MAGIC Companies frequently have thousands of large data files gathered from various teams and departments, typically using a diverse variety of formats including CSV, JSON, and XML.  Analysts often wish to extract insights from this data.
# MAGIC 
# MAGIC The classic approach to querying this data is to load it into a central database called a Data Warehouse.  This involves the time-consuming operation of designing the schema for the central database, extracting the data from the various data sources, transforming the data to fit the warehouse schema, and loading it into the central database.  The analyst can then query this enterprise warehouse directly or query smaller data marts created to optimize specific types of queries.
# MAGIC 
# MAGIC This classic Data Warehouse approach works well but requires a great deal of upfront effort to design and populate schemas.  It also limits historical data, which is restrained to only the data that fits the warehouse’s schema.
# MAGIC 
# MAGIC An alternative to this approach is the Data Lake.  A _Data Lake_:
# MAGIC 
# MAGIC * Is a storage repository that cheaply stores a vast amount of raw data in its native format
# MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
# MAGIC * Also may contain operational relational databases with live transactional data
# MAGIC 
# MAGIC Spark is ideal for querying Data Lakes as the Spark SQL query engine is capable of reading directly from the raw files and then executing SQL queries to join and aggregate the Data.
# MAGIC 
# MAGIC You will see in this lesson that once two tables are created (independent of their underlying file type), we can join them, execute nested queries, and perform other operations across our Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/yyblq4fgfl?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/yyblq4fgfl?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking at our Data Lake
# MAGIC 
# MAGIC You can start by reviewing which files are in our Data Lake.
# MAGIC 
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are some Parquet files containing 2016 crime data from several United States cities.
# MAGIC 
# MAGIC As you can see in the cell below, we have data for Boston, Chicago, New Orleans, and more.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/6v5a6qgfbb?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/6v5a6qgfbb?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %fs ls /mnt/training/crime-data-2016

# COMMAND ----------

# MAGIC %md
# MAGIC The next step in looking at the data is to create a temporary view for each file.  Recall that temporary views use a similar syntax to `CREATE TABLE` but using the command `CREATE TEMPORARY VIEW`.  Temporary views are removed once your session has ended while tables are persisted beyond a given session.
# MAGIC 
# MAGIC Start by creating a view of the data from New York and then Boston:
# MAGIC 
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | **New York**  | `CrimeDataNewYork`      | `dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet`
# MAGIC | **Boston**    | `CrimeDataBoston`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataNewYork
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet"
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrimeDataBoston
# MAGIC   USING parquet
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet"
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC With the view created, it is now possible to review the first couple records of each file.
# MAGIC 
# MAGIC Notice in the example below:
# MAGIC * The `CrimeDataNewYork` and `CrimeDataBoston` datasets use different names for the columns
# MAGIC * The data itself is formatted differently and different names are used for similar concepts
# MAGIC 
# MAGIC This is common in a Data Lake.  Often files are added to a Data Lake by different groups at different times.  While each file itself usually has clean data, there is little consistency across files.  The advantage of this strategy is that anyone can contribute information to the Data Lake and that Data Lakes scale to store arbitrarily large and diverse data.  The tradeoff for this ease in storing data is that it doesn’t have the rigid structure of a more traditional relational data model so the person querying the Data Lake will need to clean the data before extracting useful insights.
# MAGIC 
# MAGIC The alternative to a Data Lake is a Data Warehouse.  In a Data Warehouse, a committee often regulates the schema and ensures data is cleaned before being made available.  This makes querying much easier but also makes gathering the data much more expensive and time-consuming.  Many companies choose to start with a Data Lake to accumulate data.  Then, as the need arises, they clean the data and produce higher quality tables for querying.  This reduces the upfront costs while still making data easier to query over time.  These cleaned tables can even be later loaded into a formal data warehouse through nightly batch jobs.  In this way, Apache Spark can be used to manage and query both Data Lakes and Data Warehouses.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM CrimeDataNewYork

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM CrimeDataBoston

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same type of data, different structure
# MAGIC 
# MAGIC In this section, we examine crime data to figure out how to extract homicide statistics.
# MAGIC 
# MAGIC Because our data sets are pooled together in a Data Lake, each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC 
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or even "MURDER"
# MAGIC * In New York, the column is named `offenseDescription` but, in Boston, the column is named `OFFENSE_CODE_GROUP`
# MAGIC * In New York, the date of the event is in the `reportDate` column but, in Boston, there is a single column named `MONTH`

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/9mc9dtyx5u?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/9mc9dtyx5u?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC To get started, create a temporary view containing only the homicide-related rows.
# MAGIC 
# MAGIC At the same time, normalize the data structure of each table so that all the columns (and their values) line up with each other.
# MAGIC 
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC 
# MAGIC For the upcoming aggregation, you will need to alter the New York data set to include a `month` column which can be computed from the `reportDate` column using the `month()` function. Boston already has this column.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> One helpful tool for finding the offences we're looking for is using <a href="https://en.wikipedia.org/wiki/Regular_expression" target="_blank">regular expressions</a> supported by SQL
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We can also normalize the values with the `CASE`, `WHEN`, `THEN` & `ELSE` expressions but that is not required for the task at hand.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HomicidesNewYork AS
# MAGIC   SELECT month(reportDate) AS month, offenseDescription AS offense
# MAGIC   FROM CrimeDataNewYork
# MAGIC   WHERE lower(offenseDescription) LIKE 'murder%' OR lower(offenseDescription) LIKE 'homicide%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HomicidesBoston AS
# MAGIC   SELECT month, OFFENSE_CODE_GROUP AS offense
# MAGIC   FROM CrimeDataBoston
# MAGIC   WHERE lower(OFFENSE_CODE_GROUP) = 'homicide'

# COMMAND ----------

# MAGIC %md
# MAGIC You can see below that the structure of our two tables is now identical.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM HomicidesNewYork LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM HomicidesBoston LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing our data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now that we have normalized the homicide data for each city we can combine the two by taking their union.
# MAGIC 
# MAGIC When we are done, we can then aggregate that data to compute the number of homicides per month.
# MAGIC 
# MAGIC Start by creating a new view called `HomicidesBostonAndNewYork` which simply unions the result of two `SELECT` statements together.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://stackoverflow.com/questions/49925/what-is-the-difference-between-union-and-union-all">this Stack Overflow post</a> for the difference between `UNION` and `UNION ALL`

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/ld3fh1x0ig?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/ld3fh1x0ig?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HomicidesBostonAndNewYork AS
# MAGIC   SELECT * FROM HomicidesNewYork
# MAGIC     UNION ALL
# MAGIC   SELECT * FROM HomicidesBoston

# COMMAND ----------

# MAGIC %md
# MAGIC You can now see below all the data in one table:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM HomicidesBostonAndNewYork
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC And finally we can perform a simple aggregation to see the number of homicides per month:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT month, count(*) AS homicides
# MAGIC FROM HomicidesBostonAndNewYork
# MAGIC GROUP BY month
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Merge the crime data for Chicago with the data for New York and Boston and then update our final aggregation of counts-by-month.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC 
# MAGIC Create the initial view of the Chicago data.
# MAGIC 0. The source file is `dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet`
# MAGIC 0. Name the view `CrimeDataChicago`
# MAGIC 0. View the data with a simple `SELECT` statement

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.

total = spark.sql("select count(*) from CrimeDataChicago").first()[0]
dbTest("SQL-L6-crimeDataChicago-count", 267872, total)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Create a new view that normalizes the data structure.
# MAGIC 0. Name the view `HomicidesChicago`
# MAGIC 0. The table should have at least two columns: `month` and `offense`
# MAGIC 0. Filter the data to only include homicides
# MAGIC 0. View the data with a simple `SELECT` statement
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You will need to use the `month()` function to extract the month-of-the-year.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To find out which values for each offense constitutes a homicide, produce a distinct list of values from the table `CrimeDataChicago`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.

homicidesChicago = spark.sql("SELECT month, count(*) FROM HomicidesChicago GROUP BY month ORDER BY month").collect()
dbTest("SQL-L6-homicideChicago-len", 12, len(homicidesChicago))

dbTest("SQL-L6-homicideChicago-0", 54, homicidesChicago[0][1])
dbTest("SQL-L6-homicideChicago-6", 71, homicidesChicago[6][1])
dbTest("SQL-L6-homicideChicago-11", 58, homicidesChicago[11][1])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Create a new view that merges all three data sets (New York, Boston, Chicago):
# MAGIC 0. Name the view `AllHomicides`
# MAGIC 0. Use the `UNION ALL` expression introduced earlier to merge all three tables
# MAGIC   * `HomicidesNewYork`
# MAGIC   * `HomicidesBoston`
# MAGIC   * `HomicidesChicago`
# MAGIC 0. View the data with a simple `SELECT` statement
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To union three tables together, copy the previous example and just add as second `UNION` statement followed by the appropriate `SELECT` statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.

allHomicides = spark.sql("SELECT count(*) AS total FROM AllHomicides").first()[0]
dbTest("SQL-L6-allHomicides-count", 1203, allHomicides)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4
# MAGIC 
# MAGIC Create a new view that counts the number of homicides per month.
# MAGIC 0. Name the view `HomicidesByMonth`
# MAGIC 0. Rename the column `count(1)` to `homicides`
# MAGIC 0. Group the data by `month`
# MAGIC 0. Sort the data by `month`
# MAGIC 0. Count the number of records for each aggregate
# MAGIC 0. View the data with a simple `SELECT` statement

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.

allHomicides = spark.sql("SELECT * FROM HomicidesByMonth").collect()
dbTest("SQL-L6-homicidesByMonth-len", 12, len(allHomicides))

dbTest("SQL-L6-homicidesByMonth-0", 1, allHomicides[0].month)
dbTest("SQL-L6-homicidesByMonth-11", 12, allHomicides[11].month)

dbTest("SQL-L6-allHomicides-0", 83, allHomicides[0].homicides)
dbTest("SQL-L6-allHomicides-1", 68, allHomicides[1].homicides)
dbTest("SQL-L6-allHomicides-2", 72, allHomicides[2].homicides)
dbTest("SQL-L6-allHomicides-3", 76, allHomicides[3].homicides)
dbTest("SQL-L6-allHomicides-4", 105, allHomicides[4].homicides)
dbTest("SQL-L6-allHomicides-5", 120, allHomicides[5].homicides)
dbTest("SQL-L6-allHomicides-6", 116, allHomicides[6].homicides)
dbTest("SQL-L6-allHomicides-7", 144, allHomicides[7].homicides)
dbTest("SQL-L6-allHomicides-8", 109, allHomicides[8].homicides)
dbTest("SQL-L6-allHomicides-9", 109, allHomicides[9].homicides)
dbTest("SQL-L6-allHomicides-10", 111, allHomicides[10].homicides)
dbTest("SQL-L6-allHomicides-11", 90, allHomicides[11].homicides)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC * Spark SQL allows you to easily manipulate data in a Data Lake
# MAGIC * Temporary views help to save your cleaned data for downstream analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is a Data Lake?  
# MAGIC **A:** Data Lakes are a loose collection of data files gathered from various sources.  Spark loads each file as a table and then executes queries joining and aggregating these files.
# MAGIC 
# MAGIC **Q:** What are some advantages of Data Lakes over more classic Data Warehouses?  
# MAGIC **A:** Data Lakes allow for large amounts of data to be aggregated from many sources with minimal ceremony or overhead.  Data Lakes also allow for very very large files.  Powerful query engines such as Spark can read the diverse collection of files and execute complex queries efficiently.
# MAGIC 
# MAGIC **Q:** What are some advantages of Data Warehouses?  
# MAGIC **A:** Data warehouses are neatly curated to ensure data from all sources fit a common schema.  This makes them very easy to query.
# MAGIC 
# MAGIC **Q:** What's the best way to combine the advantages of Data Lakes and Data Warehouses?  
# MAGIC **A:** Start with a Data Lake.  As you query, you will discover cases where the data needs to be cleaned, combined, and made more accessible.  Create periodic Spark jobs to read these raw sources and write new "golden" tables that are cleaned and more easily queried.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> All done!</h2>
# MAGIC 
# MAGIC Thank you for your participation!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>