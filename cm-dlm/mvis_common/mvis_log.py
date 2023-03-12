'''
*****************************************************************************
*File : mvis_log.py
*Module : mvis_common 
*Purpose : Logger Library 
*Author : Sumankumar Panchal 
*Copyright : Copyright 2022, Lab to Market Innovations Private Limited
*****************************************************************************
'''

import logging
import smtplib
import time
import string
import threading
import logging.handlers as handlers


class Log:
    logger = None

    def __init__(self):
        Log.logger = logging.getLogger('WILD')
        Log.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(levelname)s: %(asctime)s: %(threadName)s:%(module)s,%(funcName)s,%(lineno)s:  %(message)s')

        # create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        Log.logger.addHandler(ch)

        # create file handler
        # Every 1 MB create a new log file
        # fh = handlers.RotatingFileHandler('wild_da/src/Processing_Module/wild.log', mode='a', maxBytes=1*1024*1024,
        #                                   backupCount=0)
        # Create log file every 4 hours
        fh = handlers.TimedRotatingFileHandler('../log/mvis.log', when='H', interval=4,
                                               backupCount=0)
        fh.setLevel(logging.WARNING)
        fh.setFormatter(formatter)
        Log.logger.addHandler(fh)

        # Create email Handler
        # Send email when critical error happens

        # host = 'smtp.gmail.com'
        # port = 587
        # destEmails = ['dhanaseelan.t@gmail.com']
        # fromEmail = 'l2mreporter@gmail.com'
        # fromPass = 'l2muser@1'
        # eh = ThreadedTlsSMTPHandler(
        #     mailhost=(host, port),
        #     fromaddr=fromEmail,
        #     toaddrs=destEmails,
        #     subject='Critical Error Has occured',
        #     credentials=(
        #         fromEmail,
        #         fromPass
        #     )
        # )
        #
        # eh.setLevel(logging.CRITICAL)
        # eh.setFormatter(formatter)
        # Log.logger.addHandler(eh)


def smtpThreadHolder(mailhost, port, username, password, fromaddr, toaddrs, msg):
    try:
        smtp = smtplib.SMTP(mailhost, port)
    except:
        logging.error("Trying to make smtp variable")
    if username:
        smtp.ehlo()  # for tls add this line
        smtp.starttls()  # for tls add this line
        smtp.ehlo()  # for tls add this line
    smtp.login(username, password)
    smtp.sendmail(fromaddr, toaddrs, msg)
    smtp.quit()


class ThreadedTlsSMTPHandler(handlers.SMTPHandler):
    def emit(self, record):
        try:
            import string  # for tls add this line
            try:
                from email.utils import formatdate
            except ImportError:
                formatdate = self.date_time

            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            msg = self.format(record)
            print(f'message: {msg}')
            # msg = ("From: %s\r\nTo"
            #        "%s\r\nSubject:"
            #        "%s\r\nDate: %s\r\n\r\n%s"
            #        % (self.fromaddr, string.join(self.toaddrs, ","), self.getSubject(record), formatdate(), msg)
            thread = threading.Thread(target = smtpThreadHolder, args = (self.mailhost, port, self.username,
                                        self.password, self.fromaddr, self.toaddrs, msg))
            thread.daemon = True
            thread.start()
        except(KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


if __name__ == '__main__':
    log = Log()
    i = 0
    Log.logger.critical(f'Test Critical message')
    while True:
        Log.logger.info(f'********************************** Test Print {i} ******************************************')
        time.sleep(1)
        i = i + 1
