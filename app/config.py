"""
    the file contains a list of available parameters, paths and default values
"""

import pathlib
from datetime import date, datetime

today = date.today().strftime("%Y%m%d")
date_time = datetime.now().strftime("%Y%m%d_%H%M%S")

# output file directory
# pathlib.Path(__file__).parents[1].joinpath('client_data')
# pathlib.Path(__file__).parents[1].joinpath('client_data')
TARGET = {"outputFile": './client_data/client_data_' + date_time + '.csv'}

# log parameters
LOG = {
    "logPath": pathlib.Path(__file__).parent.joinpath('logs'),
    "logTestPath": pathlib.Path(__file__).parents[1].joinpath('test/logs'),
    "logName": 'logfile_' + today + '.log',
    "logTestName": 'logfile_unitTest_' + today + '.log',
    "logLvl": 'DEBUG',
    # CRITICAL; ERROR; WARNING; INFO; DEBUG; NOTSET
    # => set up DEBUG to turn on debbuging logs
    "logMax": 10000,  # maxBytes
    "logBackup": 5,  # backupCount
    "logEnc": 'utf8',  # encoding
}

# default values in case lack of input parameters
DEFAULTVAL = {
    "clientData": ['src_dataset/dataset_one.csv'],
    "financialData": ['src_dataset/dataset_two.csv'],
    "colFilter": ['country'],
    "valFilter": ['United Kingdom', 'Netherlands'],
}

""" 
(pathlib.Path(__file__).parents[1].joinpath('src_dataset/dataset_one.csv'))
 str(pathlib.Path(__file__).parents[1].joinpath('src_dataset/dataset_two.csv'))
 """

# the list of colums which shoud be rename
CHANGENAME = {
    "id": 'client_identifier', "btc_a": 'bitcoin_address', "cc_t": 'credit_card_type'
}
