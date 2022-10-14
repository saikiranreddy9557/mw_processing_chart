import logging
import sys
import os


# import module
from logging.handlers import TimedRotatingFileHandler
from logging import Formatter

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

sys.path.append(BASE_DIR)
import time

from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler



BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class LoggingUtils:
    

    def __init__(self, logger_name) -> None:
        """[this is class has all fucntionaity to avail logging functionality and its modular]
        Args:
            logger_name ([str]): [name of the logger file or name of module which is availing logging fucntionaliy]
        """
        
        # self.logger= logging.getLogger(logger_name)
        
        # filename=BASE_DIR+f"/logs/logs-{logger_name}-"+ time.strftime("%Y%m%d-%H%M%S")+".log"
        
        # handler = RotatingFileHandler(filename, mode='a', maxBytes=50*1024, 
        #                          backupCount=3, encoding=None, delay=False)
        # self.logger.setLevel(logging.DEBUG)
        # formatter = Formatter(fmt='%(asctime)s - %(process)s - %(levelname)s - %(message)s')
        # handler.setFormatter(formatter)

        # # add the handler to named logger
        # self.logger.addHandler(handler)

        
        #This piece of code will create a logger.log but the log will be moved to a new log file named my_app.log.20170623 when the current day ends at midnight.


        self.logger= logging.getLogger(logger_name)

        filename=BASE_DIR+f"/logs/logs-{logger_name}"+".log"

        handler = TimedRotatingFileHandler(filename, when="midnight", interval=1)

        self.logger.setLevel(logging.DEBUG)

        formatter = Formatter(fmt='%(asctime)s - %(process)s - %(levelname)s - %(message)s')

        handler.setFormatter(formatter)

        handler.suffix = "%Y%m%d"

        self.logger.addHandler(handler)
        

        # --------------------------------------

        
        
    
    def get_logger(self):
        """
           returns logger object
        Returns:
            [LoggingUtils]: [logger object]
        """
        try:
            return self.logger
        except Exception as e:
            print(e)
      