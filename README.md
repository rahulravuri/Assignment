# Assignment
We are Provided with 2 Assignments 
#Assignment 1:
Task 1 : Create a Spark dataframe using datafile downloaded from http://jmcauley.ucsd.edu/data/amazon/links.html. 
  We Have created sparksession instance on variable spark and using spark we have created dataframe df from datafile
Task 2,3,4:Item having the least rating ,Item having most rating and Item having the longest reviews. 
  To get max rated Item and min rated item we have grouped df on basis of itemid and found average of ratings for each item, then by chaecking which avg value is max and min we found the respective items.
  To get item with max length first we created a column which stored length of revies and using this column we found max length and found respective item
Task5:  Transform: change the date MM-DD-YYYY format.
  Using to_date function pre defind in pyspark.sql.functions we have converted date format
Task6:Show a desired data frame operation which you learnt recently.
  I have created UDF operation on data frame which basically check if provided review is verified user review or not. By checking column verified it will identify if review is valid or not, once it confirms it return values (''verified user '+Userid)' , 'Unverified User '+ Userid')
Task7:Convert the whole file into Parquet file after transforming.
  We have created Parquet file using dataframe.write.parquet functionality.

  
