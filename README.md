# PySpark Learning Journey
The goal is to document my PySpark learning journey. As of Jan 1, 2025 my goal is to become a PySpark Rockstar (the go-to person for any PySpark related queries) - it's a bold claim but I believe I can do it!

---

## Table of Contents

1. [Spark Fundamentals](#spark-fundamentals)  
2. [DataFrame Operations](#dataframe-operations)  
3. [Performance Tuning](#performance-tuning)

---

## Fundamentals
Key concepts and architecture of PySpark.

### Topics Covered:
- **SparkSession**: Setup and configuration.
- **Architecture**: Understanding drivers, executors, tasks, and partitions.
- **Core Concepts**:
  - RDD vs DataFrame vs SQL APIs.
  - Lazy evaluation and Directed Acyclic Graph (DAG).
  - Transformations vs Actions.

---

## DataFrame Operations

Just getting by hands dirty here; Used Claude to generate some exercise questions for me

### Topics Covered:

- **Basic Operations**:  
  - Creating DataFrames from various sources.  
  - Common operations like `select`, `filter`, `where`, `orderBy`.  
- **Aggregations**:  
  - Techniques like `groupBy`, `agg`, `count`, `sum`, and `avg` to summarize data.  
- **Window Functions**:  
  - Using row-wise computations for ranking, running totals, and more.  
- **Joins**:  
  - Exploring inner, outer, and cross joins with examples.  
- **User-Defined Functions (UDFs)**:  
  - Writing custom functions to extend PySpark's capabilities.

---

## Performance Tuning
Following the amazing **Spark Performance Tuning** playlist by by [Afaque Ahmad](https://youtube.com/playlist?list=PLWAuYt0wgRcLCtWzUxNg4BjnYlCZNEVth&si=b9dHXW5eK9VUxPe4), this section focuses on optimizing Spark applications for efficiency and scalability.

### Topics Covered:
- Reading Spark Query Plans
- Reading Spark DAGs
- Memory Management in Spark - It's Architecture
- Shuffle Partitions
- Bucketing
- Caching
- Data Skew
- Salting for Handling Data Skew
- AQE and Broadcast Join for Handling Data Skew
- Dynamic Partition Pruning

---

## Contributing

If you have suggestions, feedback, or additional resources to share, feel free to open an issue or submit a pull request. Collaboration is always welcome!  

---

**Let's Spark the journey together! 🔥**

--- 
