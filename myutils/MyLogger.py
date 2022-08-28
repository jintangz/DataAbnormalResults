import logging
import sys


logger = logging.getLogger()
formatStr = 'asctime:        %(asctime)s\n'\
            'filename_line:  %(filename)s_[line:%(lineno)d]\n'\
            'level:          %(levelname)s\n'\
            'message:        %(message)s\n'
myFormatter = logging.Formatter(formatStr)

streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(fmt=myFormatter)
logger.addHandler(streamHandler)
fileHandler = logging.FileHandler(r'D:\CodeProject\pythonProject\log\log.log',mode='w', encoding='utf8')
fileHandler.setFormatter(fmt=myFormatter)
logger.addHandler(fileHandler)

logger.setLevel(logging.INFO)
logFormat = logging.Formatter(formatStr)

if __name__ == '__main__':
    logger.info("test")
    logger.warning("debug")
