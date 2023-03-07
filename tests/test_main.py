""" 
Use the following package for Spark tests - https://github.com/MrPowers/chispa
run test with command: python3 -m pytest
"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from chispa.schema_comparer import assert_schema_equality

from app.config import *  # DEFAULTVAL
from app.functions import parse_input_args, read_csv

spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()
spark.sparkContext.setLogLevel("OFF")


def test_foo_bar():
    assert True


def test_parse_input_args_relying_on_default_values():
    """
    Testing the function that is responsible for downloading initial
    arguments such as file paths or filtering conditions.

    Data and data Types of default values comparison
    """
    # parameters from function
    args = parse_input_args(["-ds1", "/src/testest", "-v", "test value"])
    
    # default values 
    assert args.dataset2 == DEFAULTVAL["financialData"]
    assert args.column == DEFAULTVAL["colFilter"]
    
    # data type
    assert isinstance(args.dataset2[0], str)
    assert isinstance(args.column[0], str)


def test_read_csv_Type_File():
    """ 
    Testing the function that is responsible for downloading csv files

    Data/schemas Types comparison
    """

    # Test client data structured
    testSchema1 = (
        StructType()
        .add("id", StringType(), True)
        .add("first_name", StringType(), True)
        .add("last_name", StringType(), True)
        .add("email", StringType(), True)
        .add("country", StringType(), True)
    )
    # Test financial data structured
    testSchema2 = (
        StructType()
        .add("id", StringType(), True)
        .add("btc_a", StringType(), True)
        .add("cc_t", StringType(), True)
        .add("cc_n", StringType(), True)
    )

    # data set path
    file_path1 = str(Path(__file__).parent.parent.joinpath('src_dataset/dataset_one.csv'))
    file_path2 = str(Path(__file__).parent.parent.joinpath('src_dataset/dataset_two.csv'))      
    
    # run main function
    new_df1 = read_csv(file_path1, ",")
    new_df2 = read_csv(file_path2, ",")

    assert_schema_equality(new_df1.schema, testSchema1)
    assert_schema_equality(new_df2.schema, testSchema2)


"""
def filterDataIsIn(src: DataFrame , colnm: str, condition: list ):  
    try:
        df = src.filter(upper(col(colnm)).isin(condition))  
    except Exception as e:
        logs.error('Error in filterDataIsIn()')
        logs.error( e )
    else:
        logs.info('Narrowed data by column: "%s" and values: %s',colnm, condition)
        return df

def changeColName(src: DataFrame , old: str, new: str):
    try:
        df = src.withColumnRenamed(old, new)  
    except Exception as e:
        logs.error('Error in changeColName()')
        logs.error( e )
    else:
        logs.info('Changed columns in dataFrame: %s => %s', old, new)
        return df

def checkColExist (src: DataFrame , col: str):
    logs.info('Column "%s" exist: %s',col,  (col in src.columns) ) 
    if col in src.columns:
        logs.info('Column "%s" exist in clientFinancialDataDf DataFrame.', col)  
    else:
        logs.error('Error: Column "%s" not exist in clientFinancialDataDf', col)  
        logs.error('Error: Wrong column name. Column does not exist in the data files')  
        logs.error('Error: Check the correctness of the given column')         
        sys.exit(1)
        
        """


# testParseInputArgsType ()
# testParseInputArgsVal ()
# testread_csvTypeFile1()
