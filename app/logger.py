""" 
    logging settings file
"""
import logging
from logging.handlers import RotatingFileHandler
from app.config import LOG


def log_init(
    lvl: str, max_val: int, backup: int, enc: str, log_path: str, log_name: str
):
    """loading data from csv file as pyspark dataframe.
       the file separator taken from the parameter
    Args:
        lvl      (str): loglevel
        max_val  (int): maxBytes
        backup   (int): backupCount
        enc      (str): encoding type
        log_path (str): log files directory
        log_name (str): log file name

    Returns:
        logger (Handler)
    """
    log_path.mkdir(exist_ok=True)

    # Creates a logger objct
    logger = logging.getLogger(__name__)

    # set log level
    logger.setLevel(lvl)

    # define file handler and set formatter
    log_formatter = logging.Formatter(
        "%(asctime)s :: %(levelname)s :: %(filename)s :: %(message)s "
    )

    log_file_handler = RotatingFileHandler(
        log_path.joinpath(log_name),
        mode="a",
        maxBytes=max_val,
        backupCount=backup,
        encoding=enc,
    )
    log_file_handler.setFormatter(log_formatter)

    # add file handler to logger
    logger.addHandler(log_file_handler)
    return logger


# Logger initialization
logs = log_init(
    LOG["logLvl"],
    LOG["logMax"],
    LOG["logBackup"],
    LOG["logEnc"],
    LOG["logPath"],
    LOG["logName"],
)

logs.info("Starting the application")
logs.info("Logger initialization completed")


"""
logging.Formatter -> available options:    
    exc_info = true  => Exception tuple (à la sys.exc_info) or, if no exception has occurred, None.
    lineno  => %(lineno)d  => Source line number where the logging call was issued (if available).
    message => %(message)s  => The logged message, computed as msg % args. This is set when Formatter.format() is invoked.
    asctime => %(asctime)s  => Human-readable time when the LogRecord was created. By default this is of the form "2003-07-08 16:49:45,896" 
    name => %(name)s   => Name of the logger used to log the call.
    pathname => %(pathname)s   => Full pathname of the source file where the logging call was issued (if available).
    filename => %(filename)s   =>  Filename portion of pathname.
    funcName => %(funcName)s  => Name of function containing the logging call.
    levelname => %(levelname)s => Text logging level for the message ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    levelno => %(levelno)s => Numeric logging level for the message (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    stack_info = true => You should not need to format this yourself.
"""
