# Databricks Certification Practice Exam

https://files.training.databricks.com/assessments/practice-exams/PracticeExam-DCADAS3-Python.pdf

## Question 1
Which of the following statements about the Spark driver is incorrect?

### Solution
B. The Spark driver is horizontally scaled to increase overall processing throughput -> False

This is because there's only 1 driver per Spark application, therefore driver node can only be vertically scaled.

Why only 1 driver per Spark app:
- Driver contains the SparkContext object, which is the entry point to Spark - it cannot be shared across different nodes in same app
- A driver is responsible for breaking down the job and assigning it to the worker nodes - if there are multiple drivers then there can be collisions in assignment

## Question 2
Which of the following describes nodes in cluster-mode Spark?

### Solution
E. Worker nodes are machines that host the executors responsible for the execution of tasks

## Question 3
Which of the following statements about slots is true?

### Solution
E. Slots are resources for parallelization within a Spark application.

Slots refer to the processing units in an executor used to run tasks parallely -> directly related to CPU cores alloted to each executor.

1 Core -> 1 Task at a time i.e. 1 slot

## Question 4
Which of the following is a combination of a block of data and a set of transformers that will run on a single executor?

### Solution
D. Task

Task is the smallest unit of execution in Spark - it is derived from a Job

It consists of a block of data and a set of transformations

## Question 5
Which of the following is a group of tasks that can be executed in parallel to compute the same set of operations on potentially multiple machines?

### Solution
E. Stage

A sequence of transformations without shuffling (narrow dependencies stay in the same stage)

Wide transformations triggers a new stage because the data needs to be shuffled

## Question 6
Which of the following describes a shuffle?

### Solution
A. A shuffle is the process by which data is compared across partitions.

## Question 7
DataFrame df is very large with a large number of partitions, more than there are executors in the cluster. Based on this situation, which of the following is incorrect? Assume there is one core per executor.

## Solution
A. Performance will be suboptimal because not all executors will be utilized at the same time. -> False

When no. input of partitions > no. of executors then
- Suboptimal performance because not all data can be processed at the same time
- Large number of shuffle connections will be involved in wide transformations
- Risk of OOM
- Overhead of managing resources 


## Question 8
Which of the following operations will trigger evaluation?

### Solution 
A. DataFrame.filter()
B. DataFrame.distinct()
C. DataFrame.intersect()
D. DataFrame.join()
✅ E. DataFrame.count()

Spark uses lazy evaluation method
filter(), distinct(), join() do not trigger the evaluation process. It is trigger on show(), collect(), count(), write()

- **Transformations (Lazy)**: `filter()`, `map()`, `distinct()`, `join()`, etc. → **Do not trigger execution immediately**  
- **Actions (Eager)**: `count()`, `collect()`, `show()`, `write()` → **Trigger execution**  

## Question 9
Which of the following describes the difference between transformations and actions?

### Solution
Transformations are business logic operations that do not induce execution while actions are execution triggers focused on returning results.

## Question 10
Which of the following DataFrame operations is always classified as a narrow transformation?

### Solution
D. select()

- **Narrow Transformations (No Shuffle | Parallel Execution)**: `filter()`, `map()`, `select()`, `sample()`  
- **Wide Transformations (Shuffle | )**: `groupBy()`, `join()`, `reduceByKey()`, `distinct()` 

## Question 11
Spark has a few different execution/deployment modes: cluster, client, and local. Which of the following describes Spark's execution/deployment mode?

### Solution
A. Spark's execution/deployment mode determines where the driver and executors are physically located when a Spark application is run

When you create a Spark Application, you have 3 options of execution modes
1. Local Mode
    - Runs on single machine (driver and worker on same machine)
    - Usually used for local testing and dev

2. Cluster Mode
    - Runs in ditributed env
    - driver runs on a cluster manager (like YARN or Mesos)
    - executors on different nodes

3. Client Mode
    - Similar to Cluster mode with the exception that the driver is on client machine
    - client machine is where the job was submitted  

## Question 12
Which of the following cluster configurations will ensure the completion of a Spark application in light of a worker node failure?

### Solution
B. They should all ensure completion because worker nodes are fault-tolerant.

What does fault-tolerant mean in Spark?
- This is done by
    - RDD lineage
    - Multiple executors / nodes that can takeover
- When a node / executor fails, Spark can recompute only the lost parts by checking RDD lineage showing all the transformations that were applied.


## Question 13
Which of the following describes out-of-memory errors in Spark?

### Solution
A. An out-of-memory error occurs when either the driver or an executor does not have enough memory to collect or process the data allocated to it.

- Two main scenarios for OOM
1. Driver OOM: When collecting too much data to the driver say using `collect()`
2. Executor OOM: When an executor can't handle the data partition size


## Question 14
Which of the following is the default storage level for persist() for a non-streaming DataFrame/Dataset?

### Solution
A. MEMORY_AND_DISK

This is the default storage level for both `cache()` and `persist()`. The difference being that in `persist` you can change the storage level.

## Question 15
Which of the following describes a broadcast variable?

## Solution
D. A broadcast variable is entirely cached on each worker node so it doesn't need to be shipped or shuffled between nodes with each stage.

The best example I can think of is when you have a large table and another small look-up table, then you can choose a broadcast join (by default in Spark if one table is less than 10MB then it becomes a broadcast join) and basically send the copy of the lookup table to all the nodes for the join to happen within the partition itself.

## Question 16
Which of the following operations is most likely to induce a skew in the size of your data's partitions?

## Solution
D. `df.coalesce(n)`

The `coalesce()` operation reduces the number of partitions. If the desired number of partitions is greater than the number of executors then it will merge the partitions within executors and avoid shuffle.

Since it only merges adjacent partitions, some partitions may end up much larger than others, leading to skewed data distribution.

If reducing the number of partitions, prefer `repartition(n)` to redistribute data evenly. Only use `coalesce(n)` if you want to reduce partitions without a full shuffle, but be aware of potential data skew.

## Question 17
Which of the following data structures are Spark DataFrames built on top of?

### Solution
C. RDDs

**Resilient Distributed Datasets Features**
- **Resilient** - If a node/executor crashes, then Spark will use it's lineage to track back the lost progress
- **Distributed** - Data is spread across multiple nodes
- **Immutable** - This is required such that you can preserve the original data and trace back the steps in case of a failure. Any transformation applied to a RDD results in a new RDD
- **Lazy Evaluation** - It follows lazy evaluation such that it can keep building an optiised plan for executing the transformation when an action is called
- **Partitioned** - dataset is also partitioned in smaller chunks for parallelization

## Question 18
Which of the following code blocks returns a DataFrame containing only column storeId and column division from DataFrame storesDF?

### Solution

```
A. storesDF.select("storeId").select("division")
B. storesDF.select(storeId, division)
C. storesDF.select("storeId", "division")
D. storesDF.select(col("storeId", "division"))
E. storesDF.select(storeId).select(division)
```

C. `storesDF.select("storeId", "division")`

## Question 19
Which of the following code blocks returns a DataFrame containing all columns from DataFrame storesDF except for column sqft and column customerSatisfaction?

### Solution
A. `storesDF.drop("sqft", "customerSatisfaction")`


❌ **B. `storesDF.select("storeId", "open", "openDate", "division")`**  
- This explicitly selects **only a subset** of columns, rather than removing specific ones.  
- If `storesDF` had additional columns, they would be missing from the result.  

❌ **C. `storesDF.select(-col(sqft), -col(customerSatisfaction))`**  
- PySpark **does not support** negating column selections like `-col(column_name)`.  
- This would result in a **syntax error**.  

❌ **D. `storesDF.drop(sqft, customerSatisfaction)`**  
- The `drop()` method requires **column names as strings**, but `sqft` and `customerSatisfaction` here are not in quotes.  
- This would cause an **undefined variable error**.  

❌ **E. `storesDF.drop(col(sqft), col(customerSatisfaction))`**  
- The `drop()` method **expects column names as strings**, not `col()` objects.  
- Using `col(sqft)` and `col(customerSatisfaction)` would cause an **argument type error**.  


## Question 20
The below code shown block contains an error. The code block is intended to return a DataFrame containing only the rows from DataFrame `storesDF` where the value in DataFrame storesDF's
"sqft" column is less than or equal to 25,000. Assume DataFrame storesDF is the only defined language variable. Identify the error

`storesDF.filter(sqft <= 25000)`

### Solution
B. The column name sqft needs to be quoted and wrapped in the col() function like
`storesDF.filter(col("sqft") <= 25000)`

`col()` helps Spark understand that an operation is being applied on a column and allows the Catalyst Optimizer to rearrange and optimise the query execution.

## Question 21
The code block shown below should return a DataFrame containing only the rows from DataFrame storesDF where the value in column sqft is less than or equal to 25,000 OR the value in column
customerSatisfaction is greater than or equal to 30. Choose the response that correctly fills in the numbered blanks within the code block to complete this task.

Code block:
storesDF.__1__(__2__ __3__ __4__)

### Solution
A.
`storesDF.filter((col("sqft")<=25000)|(col("customerSatisfaction")>=30))`

## Question 22
Which of the following operations can be used to convert a DataFrame column from one type to another type?

### Solution
A. `col().cast()`

`df.withColumn("age", col("age").cast("float"))`

## Question 23
Which of the following code blocks returns a new DataFrame with a new column sqft100 that is 1/100th of column sqft in DataFrame storesDF? 

Note that column sqft100 is not in the original
DataFrame storesDF

### Solution
D. `storesDF.withColumn("sqft100", col("sqft") / 100)`

## Question 24
Which of the following code blocks returns a new DataFrame from DataFrame storesDF where column numberOfManagers is the constant integer 1?

### Solution
C. `storesDF.withColumn("numberOfManagers", lit(1))`

`lit()` is used to create a literal constant value.

## Question 25
The code block shown below contains an error. The code block intends to return a new DataFrame where column storeCategory from DataFrame storesDF is split at the underscore character
into column storeValueCategory and column storeSizeCategory. Identify the error.

```
(storesDF.withColumn(
"storeValueCategory", col("storeCategory").split("_")[0]
).withColumn(
"storeSizeCategory", col("storeCategory").split("_")[1]
)
)
```

### Solution
B. The split() operation comes from the imported functions object. It accepts a Column object and split character as arguments. It is not a method of a Column object.

```
storesDF.withColumn(
"storeValueCategory", F.split(col("storeCategory"), "_")[0]
)
```

## Question 26
Which of the following operations can be used to split an array column into an individual DataFrame row for each element in the array?

### Solution
C. `explode()`

`df.withColumn("exploded", explode(col("arr")))`

## Question 27
Which of the following code blocks returns a new DataFrame where column storeCategory is an all-lowercase version of column storeCategory in DataFrame storesDF?

### Solution
A. `storesDF.withColumn("storeCategory", lower(col("storeCategory")))`

## Question 28
The code block shown below contains an error. The code block is intended to return a new DataFrame where column division from DataFrame storesDF has been renamed to column state and column managerName from DataFrame storesDF has been renamed to column
managerFullName. Identify the error.

```
(storesDF.withColumnRenamed("state", "division")
.withColumnRenamed("managerFullName", "managerName"))
```
### Solution
D. The first argument to operation withColumnRenamed() should be the old column name and the second argument should be the new column name

`df.withColumnRenamed(existing, new)`

There's also another method
`df.withColumnsRenamed({'old_name':'new_name', 'old_name_2':'new_name_2'})`

## Question 29
Which of the following code blocks returns a DataFrame where rows in DataFrame storesDF containing missing values in every column have been dropped?

### Solution
E. `storesDF.na.drop("all")`

With the `drop("all")` arguement, Spark will return those rows where all column values are NULL

`drop()`: drops rows containing any NULL value, this is default.

## Question 30
Which of the following operations fails to return a DataFrame where every row is unique?

### Solution
E. `DataFrame.drop_duplicates(subset = "all")`

"all" is not a valid argument for the subset parameter. Default arguement is None.

## Question 31
Which of the following code blocks will not always return the exact number of distinct values in column division?

### Solution
A. `storesDF.agg(approx_count_distinct(col("division")).alias("divisionDistinct"))`

- It uses the HyperLogLog algorithm which provides a close estimate while consuming lesser resources
- Faster and more memory efficient than countDistinct()

## Question 32
The code block shown below should return a new DataFrame with the mean of column sqft from DataFrame storesDF in column sqftMean. 

Choose the response that correctly fills in the
numbered blanks within the code block to complete this task.
Code block:
`storesDF.__1__(__2__(__3__).alias("sqftMean"))`

### Solution

`storesDF.agg(mean(col("sqft")))`

In Spark mean() and avg() are the same functions.


## Question 33
Which of the following code blocks returns the number of rows in DataFrame storesDF?

### Solution
D. `storesDF.count()`

## Question 34
Which of the following code blocks returns the sum of the values in column sqft in DataFrame storesDF grouped by distinct value in column division?

### Solution
E. `storesDF.groupBy("division").agg(sum(col("sqft")))`

## Question 35
Which of the following code blocks returns a DataFrame containing summary statistics only for column sqft in DataFrame storesDF?

### Solution
B. `storesDF.describe("sqft")`

## Question 36
Which of the following operations can be used to sort the rows of a DataFrame?

### Solution
A. `sort() and orderBy()`

```python
# Using sort()
df.sort("columnName", ascending=False)

# Using orderBy()
df.orderBy("columnName", ascending=False)
```

## Question 37
The code block shown below contains an error. The code block is intended to return a 15 percent sample of rows from DataFrame storesDF without replacement. Identify the error.

Code block:
`storesDF.sample(True, fraction = 0.15)`

### Solution
E. The first argument True sets the sampling to be with replacement.

Syntax for sample
`df.sample(withReplacement = True / False, fraction, seed (optional))`

```python
# say you want to sample 10% of the rows without replacement
df.sample(withReplacement=False, fraction=0.1)

# with replacement
df.sample(withReplacement=True, fraction=0.1, seed=42)
```


## Question 38
Which of the following operations can be used to return the top n rows from a DataFrame?

### Solution
B. `DataFrame.take(n)`

Difference between `take(n)` and `show(n)`, is that the latter only prints the rows -  it does not return anything. Whereas, `take(n)` returns the number of specifed rows

`collect()`: Used to fetch all rows to driver node

```python
# Fetch the top 5 rows as a list of Row objects
top_rows = df.take(5)

top_rows.show() # this will only print them to console
```

## Question 39
The code block shown below should extract the value for column sqft from the first row of DataFrame storesDF. Choose the response that correctly fills in the numbered blanks within the
code block to complete this task.

`__1__.__2__.__3__`

## Solution
D.
1. storesDF
2. first()
3. sqft

`first()`: returns first row of dataframe

`storesDF.first()["sqft"]`

## Question 40
Which of the following lines of code prints the schema of a DataFrame?

### Solution
D. `DataFrame.printSchema()`

## Question 41
In what order should the below lines of code be run in order to create and register a SQL UDF named "ASSESS_PERFORMANCE" using the Python function assessPerformance and apply it to
column customerSatistfaction in table stores?

### Solution
B. 1, 4
**Correct Execution Order**
- **Step 1:** Register the function correctly using `spark.udf.register("ASSESS_PERFORMANCE", assessPerformance)`.  
- **Step 2:** Execute the SQL query using `ASSESS_PERFORMANCE(customerSatisfaction)` in the `SELECT` statement.


The method of creating and registering a UDF
```python

# define a UDF
def my_custom_udf(col):
    return col*2

# register the UDF, with the udf() function
custom_udf = F.udf(my_custom_udf, StringType()) # specify the type for the return
```

UDF for Spark SQL
```python
# Register the function as a SQL UDF
spark.udf.register("MY_CUSTOM_UDF", my_custom_udf, StringType())

# Use it in a SQL query
spark.sql("SELECT MY_CUSTOM_UDF(existing_column) FROM my_table")
```


## Question 42
In what order should the below lines of code be run in order to create a Python UDF `assessPerformanceUDF()` using the integer-returning Python function `assessPerformance`
and apply it to column `customerSatisfaction` in DataFrame `storesDF`?

### Solution
A. 3, 4

```python
# register the UDF
assessPerformanceUDF = F.udf(assessPerformance, IntegerType())

# apply UDF
storesDF.withColumn('result', 
                    assessPerformanceUDF(F.col("customerSatisfaction"))
                    )
```

## Question 43
Which of the following operations can execute a SQL query on a table?

### Solution 
C. `spark.sql()`

## Question 44
Which of the following code blocks creates a single-column DataFrame from Python list `years` which is made up of integers?

### Solution
B. `spark.createDataFrame(years, IntegerType())`

## Question 45
Which of the following operations can be used to cache a DataFrame only in Spark’s memory assuming the default arguments can be updated?

### Solution
D. `DataFrame.persist()`

`cache()` has only 1 storage level - `MEMORY_AND_DISK`, it will try to store the DF on memory and spill to Disk if too big.


## Question 46
The code block shown below contains an error. The code block is intended to return a new 4-partition DataFrame from the 8-partition DataFrame storesDF without inducing a shuffle.

Identify the error.
Code block:
`storesDF.repartition(4)`

### Solution
D. The `repartition` operation induced a full shuffle. The `coalesce` operation should be used instead.

## Question 47
Which of the following code blocks will always return a new 12-partition DataFrame from the 8-partition DataFrame storesDF?

### Solution
C. `storesDF.repartition(12)`

## Question 48
Which of the following Spark config properties represents the number of partitions used in wide transformations like `join()`?

### Solution
A. `spark.sql.shuffle.partitions`

## Question 49
In what order should the below lines of code be run in order to return a DataFrame containing a column `openDateString`, a string representation of Java’s SimpleDateFormat?


Note that column `openDate` is of type integer and represents a date in the UNIX epoch format — the number of seconds since midnight on January 1st, 1970.

An example of Java's SimpleDateFormat is "Sunday, Dec 4, 2008 1:05 PM".

### Solution
B. 2, 1


**Understanding Each Line of Code**
| Line | Explanation | Issues? |
|------|------------|---------|
| **1** `storesDF.withColumn("openDateString", from_unixtime(col("openDate"), simpleDateFormat))` | ✅ Uses `from_unixtime()` correctly to format the date using `simpleDateFormat`. | Works, but `simpleDateFormat` must be defined first! |
| **2** `simpleDateFormat = "EEEE, MMM d, yyyy h:mm a"` | ✅ Defines the correct Java `SimpleDateFormat` pattern. | Required before using in Line 1. |
| **3** `storesDF.withColumn("openDateString", from_unixtime(col("openDate"), SimpleDateFormat()))` | ❌ `SimpleDateFormat()` is a **Java class**, not a PySpark function. | Incorrect usage. |
| **4** `storesDF.withColumn("openDateString", date_format(col("openDate"), simpleDateFormat))` | ❌ `date_format()` expects a **timestamp**, but `openDate` is an integer. | Wrong function choice. |
| **5** `storesDF.withColumn("openDateString", date_format(col("openDate"), SimpleDateFormat()))` | ❌ Same issue as Line 3; `SimpleDateFormat()` is not a valid argument. | Incorrect usage. |
| **6** `simpleDateFormat = "wd, MMM d, yyyy h:mm a"` | ✅ Defines a format, but it's incorrect (`wd` is not valid for weekdays). | Wrong format. |


```python
from pyspark.sql.functions import from_unixtime, col

# Step 1: Define the date format
simpleDateFormat = "EEEE, MMM d, yyyy h:mm a"

# Step 2: Convert UNIX timestamp to formatted string
storesDF = storesDF.withColumn("openDateString", from_unixtime(col("openDate"), simpleDateFormat))
```

## Question 50
Which of the following code blocks returns a DataFrame containing a column `month`, an integer representation of the month from column `openDate` from DataFrame storesDF?

Note that column `openDate` is of type integer and represents a date in the UNIX epoch format — the number of seconds since midnight on January 1st, 1970

### Solution
B. 
```python
storesDF.withColumn("openTimestamp",
                    col("openDate").cast("Timestamp"))/
                    .withColumn("month",
                                month(col("openTimestamp"))
                                )
```

## Question 51
Which of the following operations performs an inner join on two DataFrames?

### Solution
C. Standalone `join()` function

`join()` Function syntax

```python
df1.join(df2, 
        on = [cols*] ,
        how = "inner default/left/right/outer/left_semi/left_anti/cross")
```

## Question 52
Which of the following code blocks returns a new DataFrame that is the result of an outer join between DataFrame `storesDF` and DataFrame `employeesDF` on column `storeId`?

### Solution
A. `storesDF.join(employeesDF, "storeId", "outer")`


## Question 53
The below code block contains an error. The code block is intended to return a new DataFrame that is the result of an inner join between DataFrame `storesDF` and DataFrame `employeesDF` on column `storeId` and column `employeeId` which are in both DataFrames. Identify the error.

`storesDF.join(employeesDF, [col("storeId"), col("employeeId")])`

### Solution
E. The references to "storeId" and "employeeId" should not be inside the col() function — removing the col() function should result in a successful join.

## Question 54
Which of the following Spark properties is used to configure the broadcasting of a DataFrame without the use of the `broadcast()` operation?

### Solution
A. `spark.sql.autoBroadcastJoinThreshold`

Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1, broadcasting can be disabled. The default value is the same as `spark.sql.autoBroadcastJoinThreshold`. Note that, this config is used only in adaptive framework.

## Question 55
The code block shown below should return a new DataFrame that is the result of a cross join between DataFrame `storesDF` and DataFrame `employeesDF`. Choose the response that correctly fills in the numbered blanks within the code block to complete this task.
`__1__.__2__(__3__)`

### Solution
E. 
1. storesDF
2. crossJoin
3. employeesDF

`crossJoin()` Syntax

`df1.crossJoin(df2)`


## Question 56
Which of the following operations performs a position-wise union on two DataFrames?

### Solution 
E. `DataFrame.union()`

- This is equivalent to `UNION ALL` in SQL. `UNION` removes duplicates, whereas `UNION ALL` keeps all the rows including duplicates
- Both dataframes need to have the same schema (column names and types).

- Use `unionByName()` when column order differs but names are same

## Question 57
Which of the following code blocks writes DataFrame `storesDF` to file path `filePath` as parquet?

### Solution
E. `storesDF.write.parquet(filePath)`

```python
# basic syntax for write function
df.write.format('specify_format').option('key', 'value').save('path')

# direct write functions
df.write.parquet(file_path)
df.write.csv(file_path)
df.write.json(file_path)
```

## Question 58
The code block shown below contains an error. The code block is intended to write DataFrame `storesDF` to file path `filePath` as parquet and partition by values in column `division`. Identify the error.

`storesDF.write.repartition("division").parquet(filePath)`

### Solution
C. There is no `repartition()` operation for DataFrameWriter — the `partitionBy()` operation should be used instead.

`storesDF.write.partitionBy('division').parquet(filePath)`


## Question 59
Which of the following code blocks reads a parquet at the file path filePath into a DataFrame?

### Solution
D. `spark.read.parquet(filePath)`

## Question 60
Which of the following code blocks reads JSON at the file path `filePath` into a DataFrame with the specified schema `schema`?

### Solution
E. `spark.read.schema(schema).format("json").load(filePath)`


**Why the other options are incorrect:**
- **A.** `spark.read().schema(schema).format(json).load(filePath)` → Incorrect because the format should be `"json"` (as a string) instead of just `json`.
- **B.** `spark.read().schema(schema).format("json").load(filePath)` → The correct syntax for `spark.read` is `spark.read.schema()` instead of `spark.read()`. This extra `()` is not needed.
- **C.** `spark.read.schema("schema").format("json").load(filePath)` → `"schema"` should not be a string. It should be a `StructType` object.
- **D.** `spark.read.schema("schema").format("json").load(filePath)` → Similar to **C**, the schema should be a `StructType` object, not a string.



