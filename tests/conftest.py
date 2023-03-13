"""
Configuration module in the context of tests
"""
from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_init() -> SparkSession:
    """
    Fixture function for spark session

    Returns:
        SparkSession: spark session
    """
    spark = SparkSession.builder.master(
        "local").appName("chispa").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    return spark


@pytest.fixture(scope="session")
def data_list():
    """the list of test data

    Returns:
        list: test data
    """
    return ['VALUE_1', 'value_2', 'vAluE_3']


@pytest.fixture(scope="session")
def data_upper_list():
    """test data after converting to uppercase

    Returns:
        list: uppercase values
    """
    return ['VALUE_1', 'VALUE_2', 'VALUE_3']


@pytest.fixture(scope="session")
def data_lower_list():
    """test data after converting to lowercase

    Returns:
        list: lowercase values
    """
    return ['value_1', 'value_2', 'value_3']


@pytest.fixture(scope="session")
def source_dataset1(spark_init):
    """dataframe with test data

    Args:
        spark_init (SparkSession): session initialization

    Returns:
        DataFrame: Dataframe with test dataset1
    """
    data = [
        ('val_1', 'bac'),
        ('val_2', 'abc'),
        ('val_3', 'cba'),
        ('val_4', 'bac'),
        ('val_5', 'bac'),
        ('val_6', 'cba')
    ]
    column = ['col_1', 'col_2']
    df = spark_init.createDataFrame(data, column)
    return df


@pytest.fixture(scope="session")
def source_dataset2(spark_init):
    """dataframe with test data

    Args:
        spark_init (SparkSession): session initialization

    Returns:
        DataFrame: Dataframe with test dataset2
    """
    data = [
        ('val_1', 'abc', 'bac'),
        ('val_2', 'abc', 'cba'),
        ('val_3', 'bac', 'cba'),
        ('val_4', 'abc', 'abc'),
        ('val_5', 'abc', 'bac'),
        ('val_6', 'abc', 'cba')
    ]
    column = ['col_1', 'col_3', 'col_4']
    df = spark_init.createDataFrame(data, column)
    return df


@pytest.fixture(scope="session")
def joined_datasets(spark_init):
    """
    Result of joining two data sets source_dataset1 and source_dataset2 
    based on col_1 

    Args:
        spark_init (SparkSession): session initialization

    Returns:
        DataFrame: Dataframe jining data from dataset1 and dataset2
    """
    data = [
        ('val_1', 'bac', 'abc', 'bac'),
        ('val_2', 'abc', 'abc', 'cba'),
        ('val_3', 'cba', 'bac', 'cba'),
        ('val_4', 'bac', 'abc', 'abc'),
        ('val_5', 'bac', 'abc', 'bac'),
        ('val_6', 'cba', 'abc', 'cba')
    ]
    column = ['col_1', 'col_2', 'col_3', 'col_4']
    df = spark_init.createDataFrame(data, column)
    return df


@pytest.fixture(scope="session")
def source_data_filtered_by_col2(spark_init):
    """
    condition based on source_dataset dataset where col_2 in ('abc','cba')

    Args:
        spark_init (SparkSession): session initialization

    Returns:
        tuple[DataFrame, str, list]: created dataframe with data and column name with values on which dataframe will be filtered
    """
    data = [
        ('val_3', 'cba', 'bac', 'cba'),
        ('val_2', 'abc', 'abc', 'cba'),
        ('val_6', 'cba', 'abc', 'cba')
    ]
    colnm = 'col_2'
    upp_values = ['ABC', 'CBA']
    column = ['col_1', 'col_2', 'col_3', 'col_4']
    df = spark_init.createDataFrame(data, column)
    return df, colnm, upp_values


@pytest.fixture(scope="session")
def rename_datasets(spark_init):
    """
    column change name

    Args:
        spark_init (SparkSession): session initialization

    Returns:
        tuple[DataFrame, dict]: created dataframe with dictionary with old and new column name
    """    
    data = [
        ('val_1', 'bac', 'abc', 'bac'),
        ('val_2', 'abc', 'abc', 'cba'),
        ('val_3', 'cba', 'bac', 'cba'),
        ('val_4', 'bac', 'abc', 'abc'),
        ('val_5', 'bac', 'abc', 'bac'),
        ('val_6', 'cba', 'abc', 'cba')
    ]
    column = ['column_1', 'col_2', 'col_3', 'column_4']
    
    col_new_name = { "col_1": 'column_1', "col_4": 'column_4' }
    df = spark_init.createDataFrame(data, column)
    return df ,col_new_name
