# General Overview - Pyspark Assignment


[![Python 3.7](https://img.shields.io/badge/python-3.7-green.svg)](https://www.python.org/downloads/release/python-370/)
[![Spark 3.1.1](https://img.shields.io/badge/spark-3.1.1-green)](https://spark.apache.org/releases/spark-release-3-1-1.html)
[![Java 11](https://img.shields.io/badge/Java-11-green)](https://www.oracle.com/java/technologies/downloads/#java11)

#

Pyspark application for merging two datasets. One dataset contains information about the clients and the other one contains information about their financial details. Data narrowed down to selected filters given as an input parameter.

## Actions:
* loading data from csv files.
* joining loaded datasets into one based on "id" column
* data filtering based on given parameters 
* removing columns with sensitive data 
* renaming columns to be more descriptive

* generic functions for filtering data and renaming
* writing tests (chispa)
 

## Data
### Dataset - clients data: 

|id|first_name|last_name|email|country|
|--|--|--|--|--|

### Dataset - financial details: 

| id | btc_a | cc_t | cc_n |
|--|--|--|--|

### Output:
|client_identifier|email|bitcoin_address|credit_card_type|
|--|--|--|--|
 

## How to run application?
1. Setup your spark+python environment to which you will extract the project
2. Install requirements
>pip install -r requirements.txt
3. Run tests
>python3 -m pytest --import-mode=append tests/
4. Submit job with 4 arguments:
- path to client data
- path to financial data
- write the name of the column by which the data should be filtered 
- write list of the values  from the selected column separated by a space
```console
python3 __main__.py -ds1 'path/file1.csv' -ds1 'path/file2.csv' -c columnname -v value1 'value 2' ... VALue 'Value n'
```

where:
```console
  -ds1 [DATASET1 ...], --dataset1 [DATASET1 ...]
                        Name and path of CSV Data file with clients data (string value) (default: ['src_dataset/dataset_one.csv'])
  -ds2 [DATASET2 ...], --dataset2 [DATASET2 ...]
                        Name and path of CSV Data file with financial data (string value) (default: ['src_dataset/dataset_two.csv'])
  -c [COLUMN ...], --column [COLUMN ...]
                        Column name to filter on (string value) (default: ['country'])
  -v [VALUES ...], --values [VALUES ...]
                        comma-separated string values from selected column (data will be narrowed down) (default: ['United Kingdom', 'Netherlands'])
```
All arguments are optional -> if no values are given, they will be filled with default values (from config.py file)

#### **Example of run**
  - >python3 __main__.py -c country -v france

  - >python3 __main__.py -c country -v france 'United StaTES'

  - >python3 __main__.py -ds1 'src_dataset/dataset_one.csv' -ds1 'src_dataset/dataset_two.csv' -c 'country' -v 'France' 'United Kingdom'

  - >python3 __main__.py -ds1 'src_dataset/dataset_one.csv' -c id -v 1 2 3 4

5. Check the result in client_data/client_data_yyyymmdd_hhmmss.csv
6. Create source distribution file
>python3 setup.py bdist_wheel

