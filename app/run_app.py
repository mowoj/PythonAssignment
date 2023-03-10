"""
Module with main flow of the application

"""
import sys
import argparse
from app.config import CHANGENAME, TARGET, DEFAULTVAL, JOINON
from app.logger import logs
from app.sparkbuild import spark
from app.data import AppDataFrame


def parse_input_args(argv: list = None) -> argparse.Namespace:
    """
    Handling of application input arguments
        Parameters: *args
        Returns:
            argparse.Namespace: application input arguments
    """
    logs.info('Parsing input arguments: starting parse_input_args()')
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
    logs.debug(
        "Input parameters loaded. file1: %s; file2: %s; ColumnName:%s; Values:%s",
        args.dataset1,
        args.dataset2,
        args.column,
        args.values,
    )

    logs.debug('Input arguments parsered: %s', args)
    return args


def upper_values(val: list) -> list:
    """
    Returns a list of Uppercase string values.
        Parameters:
            val (list): the list of strings values
        Returns:
            upp_val (list): the list of strings values writing in capital letters
    """
    logs.info('changing values to Uppercase')
    logs.debug('Data from the list: %s', val)
    upp_val = []
    for i in val:
        upp_val.append(i.upper())
    logs.debug('Data changed to upperCase: %s', upp_val)
    return upp_val


def lower_values(val: list) -> list:
    """
    Returns a list of Lowercase string values.
        Parameters:
            val (list): the list of strings values
        Returns:
            low_val (list): the list of strings values writing in lowercase
    """
    logs.info('changing values to Lowercase')
    logs.debug('Data from the list: %s', val)
    low_val = []
    for i in val:
        low_val.append(i.lower())
    logs.debug('Data changed to lowerCase values: %s', low_val)
    return low_val


def run() -> int:
    """run()
    Returns:
        error code -> if all are correct the exic code is equal 0
    """
    # parsing input arguments
    args = parse_input_args(sys.argv[1:])
    logs.info('Input parameters loaded: finished parse_input_args()')

    logs.debug('client dataset path: %s', args.dataset1[0])

    # Create DataFrame with clients data
    client_data_df = AppDataFrame(
        name='client_data', spark_session=spark, file_path=args.dataset1[0])

    # data check
    client_data_df.count_row()
    client_data_df.column_list()

    logs.debug('financial dataset path: %s', args.dataset2[0])

    # Create DataFrame with financial data
    financial_data_df = AppDataFrame(
        name='financial_data', spark_session=spark, file_path=args.dataset2[0])

    # data check
    financial_data_df.count_row()
    financial_data_df.column_list()

    logs.debug('Joining two dataframes based on condition: %s',
               JOINON["condition"])

    # join data frame
    client_data_df.join_df(financial_data_df,
                           JOINON["condition"])

    # data check
    client_data_df.count_row()
    client_data_df.column_list()

    # list of condition changed with upperCase
    val_upper = upper_values(args.values)

    # column name changed with lowerCase
    val_lower = lower_values(args.column)
    colnm = val_lower[0]

    # check the available country name 
    client_data_df.check_col_exist(colnm)  
    
    # narrow data
    client_data_df.filter_data(colnm, val_upper)

    # rename column
    client_data_df.change_col(CHANGENAME.items())

    # saving modes
    logs.debug('check target parameters: "%s"; %s;', TARGET['outputFile'], TARGET['dropColumns'])
    client_data_df.save_to_csv(TARGET['outputFile'], TARGET['dropColumns'],',')
              
    logs.info('End of application')
    return 0
