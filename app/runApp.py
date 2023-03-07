""" 
Module with main flow of the application
"""
import py
import sys
import os
from app.config import CHANGENAME as chng, TARGET as trgt
from app.logger import logs
from app.functions import parse_input_args, read_csv, filter_data_isin
from app.functions import change_col_name, check_col_exist, upper_values


def run() -> int:
    """run()
    Returns:
        error code -> if all are correct the exic code is equal 0
    """
    # parsing input arguments
    logs.info('3  - Parsing input arguments: starting parse_input_args()')
    args = parse_input_args(sys.argv[1:])
    logs.info('4  - Input parameters loaded: finished parse_input_args()')

    # Create DataFrame with clients data
    logs.info('5  - Starting client data import: starting read_csv()')
    logs.debug('client dataset path: %s', args.dataset1[0])
    client_data_df = read_csv(args.dataset1[0], ",")
    logs.info('6  - End of client data import: finished read_csv()')
    logs.debug('client_data_df DataFrame Rows count: %s', client_data_df.count())

    # Create DataFrame with financial data
    logs.info('7 - Starting financial data import: starting read_csv()')
    logs.debug('financial dataset path: %s', args.dataset2[0])
    financial_data_df = read_csv(args.dataset2[0], ",")
    logs.info('8  - End of financial data import: finished read_csv()')
    logs.debug('financial_data_df DataFrame Rows count: %s', financial_data_df.count())

    # join data frame
    logs.info('9  - Joining two dataframes')
    clientfinancial_data_df = client_data_df.join(
        financial_data_df, client_data_df.id == financial_data_df.id, "inner"
    ).drop(financial_data_df.id)
    logs.info('10 - New datafreme created')

    # Get row count
    logs.debug(
        "clientfinancial_data_df DataFrame Rows count: %s",
        clientfinancial_data_df.count(),
    )

    # list of condition changed with upperCase
    logs.info('11 - Filtering the data')
    val_upper = upper_values(args.values)

    # name of the filtered column
    colnm = args.column[0]
    logs.debug('Data will be filtered by column  : "%s"', colnm)

    # check the available country name and values
    logs.info('12 - Check if column exist: check_col_exist()')
    col_exist = check_col_exist(clientfinancial_data_df, colnm)   
    if col_exist == True:
        logs.info('12.1 - Column "%s" exist in clientFinancialDataDf DataFrame.', colnm)
    else:
        logs.error('12.1 - Error: Column "%s" not exist in clientFinancialDataDf', colnm)
        logs.error('12.2 - Error: Wrong column name. Column does not exist in the data files')
        logs.error('12.3 - Error: Check the correctness of the given column')
        sys.exit(1)
    
    dist_val = (
        clientfinancial_data_df.select(colnm)
        .distinct()
        .rdd.flatMap(lambda x: x)
        .take(15)
    )
    logs.debug('Available first 15 distinct values from column: "%s" ("%s")', colnm, dist_val)
    
    # filtering the data in new dataFrame
    logs.info('13 - Filtering the data: filter_data_isin()')
    clientfinancial_data_df = filter_data_isin(
        clientfinancial_data_df, colnm, val_upper
    )
    logs.info('14 - Filtering the data')

    # Get row count
    logs.debug(
        'clientfinancial_data_df DataFrame Rows count after filtering: %s',
        clientfinancial_data_df.count(),
    )

    # Remove  personal identifiable information and credit card number
    logs.info('15 - Removing sensitive data from client and financial dataFrames')

    logs.debug('client_data_df DataFrame available columns: %s', client_data_df.columns)
    client_data_df = client_data_df.drop("first_name", "last_name", "country")
    logs.debug('client_data_df DataFrame available columns: %s', client_data_df.columns)

    logs.debug(
        'financial_data_df DataFrame available columns: %s', financial_data_df.columns
    )
    financial_data_df = financial_data_df.drop("cc_n")
    logs.debug(
        'financial_data_df DataFrame available columns: %s', financial_data_df.columns
    )

    logs.debug(
        'clientfinancial_data_df DataFrame available columns: %s',
        clientfinancial_data_df.columns,
    )
    clientfinancial_data_df = clientfinancial_data_df.drop(
        "first_name", "last_name", "country", "cc_n"
    )
    logs.debug(
        'clientfinancial_data_df DataFrame available columns after removal: %s',
        clientfinancial_data_df.columns,
    )

    logs.info('16 - Sensitive data removed')

    # rename column
    logs.info('17 - Columns renaming')
    for key, value in chng.items():
        clientfinancial_data_df = change_col_name(clientfinancial_data_df, key, value)
    logs.info('18 - Columns changed')

    # Saving modes
    logs.debug(
        'clientfinancial_data_df rows count: %s',
        clientfinancial_data_df.count(),
    )
    logs.debug('check :%s', trgt['outputFile'])
    logs.info('19 - Saving data to a CSV file')
    clientfinancial_data_df.write.option('header','True').option('sep',',').mode('overwrite').format('com.databricks.spark.csv').save(trgt['outputFile'])

    
    logs.info('20 - Data saved')
    logs.info('End of application')

    return 0
