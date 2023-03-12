import logging
import smtplib
import threading
import email.utils
import time
import logging.handlers as handlers
from logging.handlers import SMTPHandler


class Log:
    logger = None

    def __init__(self, module_name):
        Log.logger = logging.getLogger('module_name')
        Log.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(levelname)s: %(asctime)s: %(threadName)s:%(module)s,%(funcName)s,%(lineno)s:  %(message)s')

        # create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        Log.logger.addHandler(ch)

        # Create log file every 24 hours
        fh = handlers.TimedRotatingFileHandler('../log/' + module_name + '-logfile.log', when='S',
                                               interval=86400, backupCount=0)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        Log.logger.addHandler(fh)


if __name__ == '__main__':
    my_log = Log("wild_log")
    Log.logger.debug("debug Msg")
    Log.logger.info("Info Msg")
    Log.logger.warning("warning Msg")
    Log.logger.error("error Msg")
    time.sleep(10)
