# Assignment 1

## Task 1
Create a Spark DataFrame using a data file downloaded from [Amazon Product Data](http://jmcauley.ucsd.edu/data/amazon/links.html). Instantiate a SparkSession named `spark` and create a DataFrame `df` from the data file.

## Task 2, 3, 4
1. **Item with Least Rating:** Group the DataFrame `df` by item ID and calculate the average ratings for each item. Identify the item with the minimum average rating.
2. **Item with Most Rating:** Group the DataFrame `df` by item ID and calculate the average ratings for each item. Identify the item with the maximum average rating.
3. **Item with Longest Reviews:** Create a new column to store the length of reviews. Identify the item with the maximum review length.

## Task 5
Transform: Change the date format to MM-DD-YYYY using the `to_date` function provided in `pyspark.sql.functions`.

## Task 6
Show a DataFrame operation learned recently:
- Create a User-Defined Function (UDF) on the DataFrame. The UDF checks whether the provided review is from a verified user or not. It returns values like `'Verified User ' + UserId` or `'Unverified User ' + UserId` based on the verification status.

## Task 7
Convert the entire DataFrame into a Parquet file after performing transformations. Use the `write.parquet` functionality.

### How to Run
1. Download the Amazon Product Data from [here](http://jmcauley.ucsd.edu/data/amazon/links.html).
2. Set up a SparkSession named `spark`.
3. Execute the provided tasks sequentially in a PySpark environment.

### Note
Make sure to check the data file path and adjust it accordingly in the code.



# Assignment 2

## Task 1
Create a Spark DataFrame using a data file downloaded from [Amazon Product Data](http://jmcauley.ucsd.edu/data/amazon/links.html). Instantiate a SparkSession named `spark` and create a DataFrame `df` from the data file.

## Task 2
Transform: Change the date format to MM-DD-YYYY using the `to_date` function provided in `pyspark.sql.functions`.

## Task 3
Transform: Save the data into a table (PostgreSQL/SQL Server)

```python
# Note: Replace placeholders with your actual database connection details

#  PostgreSQL
df.write.format("jdbc").option("url", "jdbc:postgresql://your-postgres-host:your-postgres-port/your-database").option("dbtable", "your-table").option("user", "your-username").option("password", "your-password").mode("overwrite").save()
```
## Task 4
Convert the entire DataFrame into a Parquet file after performing transformations. Use the `write.parquet` functionality.
