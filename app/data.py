"""
Functions used to modification datasets
    Classes:
        AppData
        AppDataFrame
    
    Functions:
        filter_data_isin(DataFrame, str, list) -> DataFrame:
            filtering data selected column and values.
        change_col_name(DataFrame, dict) -> DataFrame:
            changing column names
         
        count_row() -> int:
            count number of records in the dataframe
        column_list() -> list:
            listed columns from dataframe
        join_df(class AppDataFrame,  str) -> None:
            joined two dataframes
        check_col_exist(str) -> bool:
            verification if column exists in dataframe
        filter_data(str, list) -> None:
            filter data by selected column and values
        change_col(dict) -> None:
            change column names 
        save_to_csv(str, list, str) -> None:
            Save data to a csv file
"""
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, upper

from app.logger import logs
from app.config import DEFAULTVAL, TARGET


class AppData:
    """
    A class to represent a generic functions

    Methods
    -------
    filter_data_isin(DataFrame, str, list):
        Returns narrowed data
    change_col_name(DataFrame, dict:  
        Returns data with renamed columns
    """

    def filter_data_isin(df: DataFrame, colnm: str, condition: list) -> DataFrame:
        """
        Returns pyspark dataframe filtering by selected column and values.
        In the output remain data which are in the list of conditions.
            Parameters:
                df   (DataFrame):  dataframe that should be filtered
                colnm      (str): column to be filtered on
                condition (list): list of values on which the dataset will be narrowed
            Returns:    
                df   (DataFrame): dataframe with narrowed data
        """
        logs.info('Filtering the data using AppData class')
        logs.debug('Data will be filtered by column  : "%s"', colnm)
        df = df.filter(upper(col(colnm)).isin(condition))
        logs.debug('Narrowed data by column: "%s" and values: %s',
                  colnm, condition)
        return df

    def change_col_name(df: DataFrame, dict_val: dict) -> DataFrame:
        """
        Returns pyspark dataframe with changed column names according to the list from the configuration file
            Parameters:
                dict_val (dict): dictionary with ole column names and new ones
            Returns:    
                df  (DataFrame): dataframe with changed column names
        """
        logs.info('Changing column names using AppData class')
        for key, value in dict_val:
            df = df.withColumnRenamed(key, value)
            logs.debug('Changed columns in dataFrame: %s => %s', key, value)
        return df


class AppDataFrame:

    """
    A class to represent a DataFrame.

    Attributes
    ----------
    name : str
        name of dataframe
    spark_session : SparkSession
        sprake session
    file_path : str
        path with the name of the download file
    separator : str
        csv file separator
    file_header : bool
        boolean value indicating whether to download the headers                
    df : pyspark.DataFrame
        dataframe with data from the CSV file
    Methods
    -------
    info(additional=""):
        Prints the person's name and age.
    """

    def __init__(
        self, name: str, spark_session: SparkSession, file_path: str,
        separator: str = DEFAULTVAL["separator"], file_header: bool = DEFAULTVAL["fileHeader"]
    ) -> None:
        """
        The init() method is a special method that Python runs automatically whenever new instance based on the class is created.
        Datasets Initizalization Returns pyspark dataframe with data from csv file.
        The file separator is taken from the parameter.
            Parameters:
                name         (str): name of dataframe
                spark_session (SparkSession):  sprake session
                file_path    (str): path with the name of the download file
                separator    (str): csv file separator
                file_header (bool): boolean value indicating whether to download the headers                
                df     (DataFrame): dataframe with data from the CSV file
        """
        logs.info('Starting dataset import')
        self.name: str = name
        self.spark: SparkSession = spark_session

        logs.info('Starting dataset import from file')
        logs.debug('Starting dataset import from file: %s', file_path)
        self.df = self.spark.read.csv(
            path=file_path, sep=separator, header=file_header)

        logs.debug('Available columns in new dataframe: %s', self.df.columns)
        for col in self.df.columns:
            self.df = self.df.withColumnRenamed(col, col.lower())
        logs.debug('Column names changed to lowercase: %s', self.df.columns)

        logs.info('DataFrame initialization complete: %s', name)

    def count_row(self) -> int:
        """
        Returns counted number of records in the dataframe
            Returns:
                int: number of records in dataframe
        """
        logs.debug('%s DataFrame Rows count: %s', self.name, self.df.count())
        return self.df.count()

    def column_list(self) -> list:
        """
        Returns a list of columns contained in a dataframe
            Returns:
                list: column names from dataframe
        """
        logs.debug('%s DataFrame Rows count: %s', self.name, self.df.columns)
        return self.df.columns

    def join_df(self, second_df: 'AppDataFrame',  condition: str) -> None:
        """
        Returns new dataframe builded as join of two other dataframes
            Parameters:
                second_df (DataFrame): the second dataframe which will be joined
                condition       (str):  condition on which two dataframes will be joined
                
        """
        logs.info('Joining two dataFrames')
        logs.debug('DataFrames "%s" and "%s" will be joined based on "%s"',
                   self.name, second_df.name, condition)
        self.df= self.df.join(second_df.df, condition)
        self.name = self.name + '_' + second_df.name
        logs.info('New dataFrame created')

    def check_col_exist(self, colnm: str) -> bool:
        """
        Returns information whether it is true that given column exists in a given dataframe.
        If column does not exist break the application
            Parameters:
                colnm  (str): name of the column to check if it exists
        """
        logs.info('Check if column exist')
        logs.debug('Column "%s" exist: %s', colnm,
                  (colnm in self.df.columns))
        if colnm in self.df.columns:
            logs.info( 'Column exist in DataFrame')
            return True
        else:
            logs.error('Error: Column "%s" not exist in dataFrame %s', colnm, self.name)
            logs.error('Error: Wrong column name. Check the correctness of the given column')
            sys.exit(1)


    def filter_data(self, colnm: str, condition: list) -> None:
        """
        Use AppData Class to filter data by selected column and values.
        In the output remain data which are in the list of conditions.
            Parameters:
                colnm      (str): column to be filtered on
                condition (list): list of values on which the dataset will be narrowed
        """
        logs.info('Filtering the data in the main dataframe')
        self.df = AppData.filter_data_isin(self.df, colnm, condition)
        logs.info('data has been filtered. remaining number of records: %s', self.count_row())

    def change_col(self, dict_val: dict) -> None:
        """
        Use AppData Class to changed column names according to the list from the configuration file
            Parameters:
                dict_val (dict): dictionary with ole column names and new ones
        """
        logs.info('Changing column name in the main dataframe')
        self.df = AppData.change_col_name(self.df, dict_val)
        logs.info('Columns changed in the main dataframe')

    def save_to_csv(self, file_path: str = TARGET['outputFile'], col_list: list = TARGET['finalColumns'], separator: str = ',') -> None:
        """
        Saves prepared data to a csv file to indicated location
            Parameters:
                file_path (str): path to save the output file 
                col_list (list): list of columns to remove before saving
                separator (str): csv file separator
        """
        logs.info('Removing columns from dataframe. only the final ones remain')
        self.df = self.df.drop(*col_list) #removing more then one
        
        logs.info('Saving data to a CSV file')
        self.df.write.option('header', 'True').option('sep', separator).mode(
            'overwrite').format('com.databricks.spark.csv').save(file_path)
        logs.info('Data saved')
