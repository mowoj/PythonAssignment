""" 
    Functions used to modification datasets
"""
import sys
import argparse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

from app.config import DEFAULTVAL
from app.logger import logs
from app.sparkbuild import spark


def parse_input_args(argv: list = None) -> argparse.Namespace:
    """handling of application input arguments
    Args: *args
    Returns:
        application input arguments
    """
    parser = argparse.ArgumentParser(
        description="PyAppCodacAssignment",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-ds1",
        "--dataset1",
        nargs="*",
        default=DEFAULTVAL["clientData"],
        help="Name and path of CSV Data file with clients data (string value)",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-ds2",
        "--dataset2",
        nargs="*",
        default=DEFAULTVAL["financialData"],
        help="Name and path of CSV Data file with financial data (string value)",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-c",
        "--column",
        nargs="*",
        default=DEFAULTVAL["colFilter"],
        help="Column name to filter on (string value)",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-v",
        "--values",
        nargs="*",
        default=DEFAULTVAL["valFilter"],
        help="comma-separated string values from selected column (data will be narrowed down)",
        type=str,
        required=False,
    )

    args = parser.parse_args(argv)
    logs.info(
        "Input parameters loaded. file1: %s; file2: %s; ColumnName:%s; Values:%s",
        args.dataset1,
        args.dataset2,
        args.column,
        args.values,
    )

    return args


def read_csv(file_path: str, separator: str) -> DataFrame:
    """loading data from csv file as pyspark dataframe.
       the file separator taken from the parameter.
    Args:
        file_path  (str): path with the name of the download file
        separator (str): csv file separator
    Returns:
        pyspark.DataFrame: data from the CSV file
    """
    new_df = spark.read.csv(path=file_path, sep=separator, header=True)
    logs.info('Import data from file: %s', file_path)
    return new_df


def filter_data_isin(src: DataFrame, colnm: str, condition: list) -> DataFrame:
    """filtering data by selected column and values.
       In the  output remain data which are in the list of conditions.
    Args:
        src  (DataFrame): dataframe with data to filter
        colnm      (str): name of the column to filter on
        condition (list): values from selected column used to narrow down the data

    Returns:
        pyspark.DataFrame: narrowed data
    """
    new_df = src.filter(upper(col(colnm)).isin(condition))
    logs.info('Narrowed data by column: "%s" and values: %s', colnm, condition)
    return new_df


def change_col_name(src: DataFrame, old: str, new: str) -> DataFrame:
    """renaming column names according to the list from the configuration file
    Args:
        src (DataFrame): dataframe with data in which the columns should be renamed
        old       (str): old column name
        new       (str): new column name

    Returns:
        pyspark.DataFrame: data with changed columns
    """
    new_df = src.withColumnRenamed(old, new)
    logs.info('Changed columns in dataFrame: %s => %s', old, new)
    return new_df


def check_col_exist(src: DataFrame, colnm: str) -> bool:
    """renaming column names according to the list from the configuration file
    Args:
        src (DataFrame): dataframe with data in which the columns should exist
        col       (str): column name

    """
    logs.info('Column "%s" exist: %s', colnm, (colnm in src.columns))
    if colnm in src.columns:
        return True
    else:
        return False


def upper_values(val: list) -> list:
    """Changing string values to capital letters
    Args:
        val (list): the list of strings values
    Returns:
        upp_val (list): the list of strings values writing in capital letters
    """
    value = val
    logs.debug('Data narrowed to values: %s', value)
    upp_val = []
    for i in value:
        upp_val.append(i.upper())
    logs.debug('Data narrowed to upperCase values: %s', upp_val)
    return upp_val
