# Parallelization of SQL statements on client side

## Introduction

This project is a attempt to create a simple idiom to make explict parallelization of 
SQL statements on client side. 

The project is intended only to retrieve data from the database.



## Usage

Make a SQL statement with named style parameters with the rules above:

* :param_[name]: This idiom is used to hint a dynamic parameter without parallel hint
* :param_[name]_PARALLEL : This idiom is used to hint a parameter with parallel hint


A SQL statement example:
```sql
SELECT *
FROM table
WHERE val_1 = :param_1 and val_2 = :param_2_PARALLEL
```

The example above use 2 dynamic parameters, :param_1 is a normal named parameter and :param_2_PARALLEL 
is intented to execute in parallel with a list of values


The python code:
```python
from parallel_queries import execute_query_in_parallel

stmt = """
SELECT *
FROM table
WHERE val_1 = :param_1 and val_2 = :param_2_PARALLEL
"""

result = execute_query_in_parallel(engine, stmt, {'param_1': val_param_1, 'param_2_PARALLEL': list_param_2}, n_jobs=4)

```

*Note¹*: the remove of ':' in the keys of the dict parameters

*Note²*: Tested with sqlite and pymssql 

