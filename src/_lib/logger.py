import logging
from time import strftime

def setup_logging(path_logfile:str, name_logfile:str, add_date_to_name = False ,debug = False):
    
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    if add_date_to_name:
        name_logfile = f"{name_logfile}{strftime("%Y%m%d")}"

    logging.basicConfig(
        filename = path_logfile + name_logfile + ".log",
        level = level,
        style = "{",
        format = "{asctime} [{name:4}] [{levelname:8}] {message}",
        datefmt = "%d.%m.%Y %H:%M:%S"
     )
    


